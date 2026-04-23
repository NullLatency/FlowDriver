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
}

// NewGoogleBackend creates a new GoogleBackend.
func NewGoogleBackend(client *http.Client, saPath, folderID string) *GoogleBackend {
	return &GoogleBackend{
		httpClient: client,
		saPath:     saPath,
		folderID:   folderID,
		fileIDs:    make(map[string]string),
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
	req, err := http.NewRequestWithContext(ctx, "POST", b.tokenURI, strings.NewReader(v.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := b.httpClient.Do(req)
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

func (b *GoogleBackend) Upload(ctx context.Context, filename string, data []byte) error {
	tok, err := b.getValidToken(ctx)
	if err != nil {
		return err
	}

	var metaBuf bytes.Buffer
	metaWriter := multipart.NewWriter(&metaBuf)

	// Part 1: Metadata
	h := make(textproto.MIMEHeader)
	h.Set("Content-Type", "application/json; charset=UTF-8")
	part1, _ := metaWriter.CreatePart(h)
	meta := map[string]interface{}{
		"name": filename,
	}
	if b.folderID != "" {
		meta["parents"] = []string{b.folderID}
	}
	json.NewEncoder(part1).Encode(meta)

	// Part 2: Content
	h = make(textproto.MIMEHeader)
	h.Set("Content-Type", "application/octet-stream")
	part2, _ := metaWriter.CreatePart(h)
	part2.Write(data)
	metaWriter.Close()

	req, err := http.NewRequestWithContext(ctx, "POST", "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart", &metaBuf)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", metaWriter.FormDataContentType())

	resp, err := b.httpClient.Do(req)
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

	q := fmt.Sprintf("name contains '%s'", prefix)
	if b.folderID != "" {
		q += fmt.Sprintf(" and '%s' in parents", b.folderID)
	}

	u, _ := url.Parse("https://www.googleapis.com/drive/v3/files")
	v := u.Query()
	v.Set("q", q)
	v.Set("fields", "files(id, name)")
	u.RawQuery = v.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list returned %d: %s", resp.StatusCode, string(body))
	}

	var resData struct {
		Files []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"files"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&resData); err != nil {
		return nil, err
	}

	b.fileIdsMu.Lock()
	var names []string
	for _, f := range resData.Files {
		// Only collect exact prefix matches client-side just in case
		if strings.HasPrefix(f.Name, prefix) {
			b.fileIDs[f.Name] = f.ID
			names = append(names, f.Name)
		}
	}
	b.fileIdsMu.Unlock()

	return names, nil
}

func (b *GoogleBackend) Download(ctx context.Context, filename string) ([]byte, error) {
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

	req, err := http.NewRequestWithContext(ctx, "GET", "https://www.googleapis.com/drive/v3/files/"+fileID+"?alt=media", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("download returned %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
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

	req, err := http.NewRequestWithContext(ctx, "DELETE", "https://www.googleapis.com/drive/v3/files/"+fileID, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)

	resp, err := b.httpClient.Do(req)
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
