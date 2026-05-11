package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type oauthClientJSON struct {
	Installed struct {
		ClientID     string   `json:"client_id"`
		ClientSecret string   `json:"client_secret"`
		AuthURI      string   `json:"auth_uri"`
		TokenURI     string   `json:"token_uri"`
		RedirectURIs []string `json:"redirect_uris"`
	} `json:"installed"`
}

type tokenCache struct {
	RefreshToken string `json:"refresh_token"`
}

// GoogleBackend implements Backend using raw Google Drive REST APIs
// via a 3-legged OAuth2 Client structure.
type GoogleBackend struct {
	httpClient *http.Client
	saPath     string // Path to client_secret_*.json
	folderID   string

	clientID     string
	clientSecret string
	tokenURI     string
	redirectURI  string

	token        string
	refreshToken string
	tokenEx      time.Time
	mu           sync.Mutex

	// map from filename to Google Drive file ID
	fileIDs   map[string]string
	fileIdsMu sync.RWMutex

	retryMax    int
	retryBaseMs int
}

// NewGoogleBackend creates a new GoogleBackend.
func NewGoogleBackend(client *http.Client, saPath, folderID string) *GoogleBackend {
	return &GoogleBackend{
		httpClient:  client,
		saPath:      saPath,
		folderID:    folderID,
		fileIDs:     make(map[string]string),
		retryMax:    3,
		retryBaseMs: 300,
	}
}

// SetRetryPolicy configures retries for transient Google API failures.
func (b *GoogleBackend) SetRetryPolicy(maxAttempts, baseDelayMs int) {
	if maxAttempts > 0 {
		b.retryMax = maxAttempts
	}
	if baseDelayMs > 0 {
		b.retryBaseMs = baseDelayMs
	}
}

func (b *GoogleBackend) Login(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Parse Client Secret JSON
	data, err := os.ReadFile(b.saPath)
	if err != nil {
		return fmt.Errorf("failed to read Client Secret JSON %s: %w", b.saPath, err)
	}
	var oauthJSON oauthClientJSON
	if err := json.Unmarshal(data, &oauthJSON); err != nil {
		return fmt.Errorf("failed to unmarshal Client Secret JSON: %w", err)
	}

	b.clientID = oauthJSON.Installed.ClientID
	b.clientSecret = oauthJSON.Installed.ClientSecret
	// Hardcode TokenURI to www.googleapis.com to ensure domain fronting via Host header matches
	b.tokenURI = "https://www.googleapis.com/oauth2/v4/token"
	authURI := oauthJSON.Installed.AuthURI
	if len(oauthJSON.Installed.RedirectURIs) > 0 {
		b.redirectURI = oauthJSON.Installed.RedirectURIs[0]
	} else {
		b.redirectURI = "http://localhost"
	}

	tokenCachePath := b.saPath + ".token"

	// Check if we have a saved refresh token
	if cacheData, err := os.ReadFile(tokenCachePath); err == nil {
		var cache tokenCache
		if err := json.Unmarshal(cacheData, &cache); err == nil && cache.RefreshToken != "" {
			b.refreshToken = cache.RefreshToken
			return b.refreshAccessToken(ctx)
		}
	}

	// Interactive 3-Legged Flow
	link := fmt.Sprintf("%s?client_id=%s&redirect_uri=%s&response_type=code&scope=https://www.googleapis.com/auth/drive.file&access_type=offline",
		authURI, url.QueryEscape(b.clientID), url.QueryEscape(b.redirectURI))

	fmt.Printf("\n==================== OAUTH AUTHENTICATION REQUIRED ====================\n")
	fmt.Printf("1. Please open this URL in your web browser:\n\n%s\n\n", link)
	fmt.Printf("2. Authenticate and accept the permissions.\n")
	fmt.Printf("3. The browser will redirect to something like %s/?code=4/1AX4X...\n", b.redirectURI)
	fmt.Printf("   (It's okay if the browser says 'Unable to connect' or 'Site can't be reached')\n")
	fmt.Printf("4. Please copy the FULL redirected URL from your browser's address bar and paste it below:\n")
	fmt.Printf("\nEnter URL or Code: ")

	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	// Extract code
	code := input
	if strings.HasPrefix(input, "http") {
		u, err := url.Parse(input)
		if err == nil {
			qCode := u.Query().Get("code")
			if qCode != "" {
				code = qCode
			}
		}
	}

	if code == "" {
		return fmt.Errorf("invalid authorization code")
	}

	fmt.Printf("Trading code for tokens via domain-fronted proxy mapping...\n")
	if err := b.exchangeCode(ctx, code); err != nil {
		return err
	}

	// Save the refresh token to sidestep interactive flow next boot
	cache := tokenCache{RefreshToken: b.refreshToken}
	cacheBytes, _ := json.MarshalIndent(cache, "", "  ")
	if err := os.WriteFile(tokenCachePath, cacheBytes, 0600); err != nil {
		fmt.Printf("WARNING: Failed to save refresh token to %s: %v\n", tokenCachePath, err)
	} else {
		fmt.Printf("Saved refresh token to %s. Future startups will be silent.\n", tokenCachePath)
	}

	fmt.Printf("OAuth Authentication Successful!\n=======================================================================\n\n")
	return nil
}

func (b *GoogleBackend) exchangeCode(ctx context.Context, code string) error {
	v := url.Values{}
	v.Set("grant_type", "authorization_code")
	v.Set("code", code)
	v.Set("client_id", b.clientID)
	v.Set("client_secret", b.clientSecret)
	v.Set("redirect_uri", b.redirectURI)

	return b.executeTokenRequest(ctx, v)
}

func (b *GoogleBackend) refreshAccessToken(ctx context.Context) error {
	v := url.Values{}
	v.Set("grant_type", "refresh_token")
	v.Set("refresh_token", b.refreshToken)
	v.Set("client_id", b.clientID)
	v.Set("client_secret", b.clientSecret)

	return b.executeTokenRequest(ctx, v)
}

func (b *GoogleBackend) executeTokenRequest(ctx context.Context, v url.Values) error {
	body := v.Encode()
	resp, err := b.doRetry(ctx, "token", func() (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", b.tokenURI, strings.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return b.httpClient.Do(req)
	})
	if err != nil {
		return fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("token request returned %d: %s", resp.StatusCode, string(body))
	}

	var resData struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&resData); err != nil {
		return fmt.Errorf("failed to decode token response: %w", err)
	}

	b.token = resData.AccessToken
	if resData.RefreshToken != "" {
		b.refreshToken = resData.RefreshToken
	}
	b.tokenEx = time.Now().Add(time.Duration(resData.ExpiresIn-60) * time.Second) // 60s buffer
	return nil
}

func (b *GoogleBackend) getValidToken(ctx context.Context) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if time.Now().After(b.tokenEx) {
		if err := b.refreshAccessToken(ctx); err != nil {
			return "", err
		}
	}
	return b.token, nil
}

func (b *GoogleBackend) Upload(ctx context.Context, filename string, data io.Reader) error {
	tok, err := b.getValidToken(ctx)
	if err != nil {
		return err
	}

	payload, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	resp, err := b.doRetry(ctx, "upload", func() (*http.Response, error) {
		var body bytes.Buffer
		metaWriter := multipart.NewWriter(&body)

		h := make(textproto.MIMEHeader)
		h.Set("Content-Type", "application/json; charset=UTF-8")
		part1, err := metaWriter.CreatePart(h)
		if err != nil {
			return nil, err
		}
		meta := map[string]interface{}{
			"name": filename,
		}
		if b.folderID != "" {
			meta["parents"] = []string{b.folderID}
		}
		if err := json.NewEncoder(part1).Encode(meta); err != nil {
			return nil, err
		}

		h = make(textproto.MIMEHeader)
		h.Set("Content-Type", "application/octet-stream")
		part2, err := metaWriter.CreatePart(h)
		if err != nil {
			return nil, err
		}
		if _, err := part2.Write(payload); err != nil {
			return nil, err
		}
		if err := metaWriter.Close(); err != nil {
			return nil, err
		}

		req, err := http.NewRequestWithContext(ctx, "POST", "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart", bytes.NewReader(body.Bytes()))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", metaWriter.FormDataContentType())
		return b.httpClient.Do(req)
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (b *GoogleBackend) ListQuery(ctx context.Context, prefix string) ([]string, error) {
	tok, err := b.getValidToken(ctx)
	if err != nil {
		return nil, err
	}

	q := fmt.Sprintf("name contains '%s' and trashed = false", escapeDriveQuery(prefix))
	if b.folderID != "" {
		q += fmt.Sprintf(" and '%s' in parents", b.folderID)
	}

	var names []string
	pageToken := ""
	for {
		u, _ := url.Parse("https://www.googleapis.com/drive/v3/files")
		v := u.Query()
		v.Set("q", q)
		v.Set("fields", "nextPageToken, files(id, name)")
		v.Set("pageSize", "1000")
		v.Set("spaces", "drive")
		if pageToken != "" {
			v.Set("pageToken", pageToken)
		}
		u.RawQuery = v.Encode()

		resp, err := b.doRetry(ctx, "list", func() (*http.Response, error) {
			req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
			if err != nil {
				return nil, err
			}
			req.Header.Set("Authorization", "Bearer "+tok)
			return b.httpClient.Do(req)
		})
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("list returned %d: %s", resp.StatusCode, string(body))
		}

		var resData struct {
			NextPageToken string `json:"nextPageToken"`
			Files         []struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"files"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&resData); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()

		b.fileIdsMu.Lock()
		// SAFETY: Prevent fileIDs map from infinite growth
		if len(b.fileIDs) > 5000 {
			b.fileIDs = make(map[string]string)
		}
		for _, f := range resData.Files {
			// Only collect exact prefix matches client-side just in case
			if strings.HasPrefix(f.Name, prefix) {
				b.fileIDs[f.Name] = f.ID
				names = append(names, f.Name)
			}
		}
		b.fileIdsMu.Unlock()

		if resData.NextPageToken == "" {
			break
		}
		pageToken = resData.NextPageToken
	}

	return names, nil
}

func (b *GoogleBackend) Download(ctx context.Context, filename string) (io.ReadCloser, error) {
	b.fileIdsMu.RLock()
	fileID, ok := b.fileIDs[filename]
	b.fileIdsMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("file-id mapping not found for %s", filename)
	}

	tok, err := b.getValidToken(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := b.doRetry(ctx, "download", func() (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, "GET", "https://www.googleapis.com/drive/v3/files/"+fileID+"?alt=media", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		return b.httpClient.Do(req)
	})
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("download returned %d: %s", resp.StatusCode, string(body))
	}

	return resp.Body, nil
}

func (b *GoogleBackend) Delete(ctx context.Context, filename string) error {
	b.fileIdsMu.RLock()
	fileID, ok := b.fileIDs[filename]
	b.fileIdsMu.RUnlock()

	if !ok {
		return fmt.Errorf("file-id mapping not found for %s", filename)
	}

	tok, err := b.getValidToken(ctx)
	if err != nil {
		return err
	}

	resp, err := b.doRetry(ctx, "delete", func() (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, "DELETE", "https://www.googleapis.com/drive/v3/files/"+fileID, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		return b.httpClient.Do(req)
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete returned %d: %s", resp.StatusCode, string(body))
	}

	b.fileIdsMu.Lock()
	delete(b.fileIDs, filename)
	b.fileIdsMu.Unlock()

	return nil
}

func (b *GoogleBackend) CreateFolder(ctx context.Context, name string) (string, error) {
	tok, err := b.getValidToken(ctx)
	if err != nil {
		return "", err
	}

	meta := map[string]interface{}{
		"name":     name,
		"mimeType": "application/vnd.google-apps.folder",
	}
	body, _ := json.Marshal(meta)

	resp, err := b.doRetry(ctx, "create folder", func() (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", "https://www.googleapis.com/drive/v3/files", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		return b.httpClient.Do(req)
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		resBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("create folder returned %d: %s", resp.StatusCode, string(resBody))
	}

	var resData struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&resData); err != nil {
		return "", err
	}

	b.folderID = resData.ID
	return resData.ID, nil
}

func (b *GoogleBackend) FindFolder(ctx context.Context, name string) (string, error) {
	tok, err := b.getValidToken(ctx)
	if err != nil {
		return "", err
	}

	q := fmt.Sprintf("name = '%s' and mimeType = 'application/vnd.google-apps.folder' and trashed = false", name)
	u, _ := url.Parse("https://www.googleapis.com/drive/v3/files")
	v := u.Query()
	v.Set("q", q)
	v.Set("fields", "files(id, name)")
	u.RawQuery = v.Encode()

	resp, err := b.doRetry(ctx, "find folder", func() (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		return b.httpClient.Do(req)
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("find folder returned %d: %s", resp.StatusCode, string(body))
	}

	var resData struct {
		Files []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"files"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&resData); err != nil {
		return "", err
	}

	if len(resData.Files) > 0 {
		b.folderID = resData.Files[0].ID
		return resData.Files[0].ID, nil
	}
	return "", nil
}

func (b *GoogleBackend) doRetry(ctx context.Context, operation string, fn func() (*http.Response, error)) (*http.Response, error) {
	attempts := b.retryMax
	if attempts <= 0 {
		attempts = 1
	}
	baseDelay := time.Duration(b.retryBaseMs) * time.Millisecond
	if baseDelay <= 0 {
		baseDelay = 300 * time.Millisecond
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		resp, err := fn()
		if err == nil && resp != nil && !shouldRetryStatus(resp.StatusCode) {
			return resp, nil
		}

		if resp != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			if err == nil {
				lastErr = fmt.Errorf("%s returned %d: %s", operation, resp.StatusCode, strings.TrimSpace(string(body)))
			}
		}
		if err != nil {
			lastErr = err
		}
		if attempt == attempts {
			break
		}

		delay := baseDelay << (attempt - 1)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, lastErr
}

func shouldRetryStatus(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusRequestTimeout || status >= 500
}

func escapeDriveQuery(value string) string {
	return strings.ReplaceAll(value, "'", "\\'")
}
