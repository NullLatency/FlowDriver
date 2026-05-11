package app

import (
	"fmt"

	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/httpclient"
	"github.com/NullLatency/flow-driver/internal/storage"
)

func BuildBackend(appCfg *config.AppConfig, gcPath string) (storage.Backend, error) {
	if appCfg.StorageType != "google" {
		return storage.NewLocalBackend(appCfg.LocalDir)
	}

	if len(appCfg.GoogleLanes) == 0 {
		customHTTPClient := httpclient.NewCustomClient(appCfg.Transport)
		googleBackend := storage.NewGoogleBackend(customHTTPClient, gcPath, appCfg.GoogleFolderID)
		googleBackend.SetRetryPolicy(appCfg.StorageRetryMax, appCfg.StorageRetryBaseMs)
		return googleBackend, nil
	}

	backends := make([]storage.Backend, 0, len(appCfg.GoogleLanes))
	for idx, lane := range appCfg.GoogleLanes {
		laneCredentials := lane.CredentialsPath
		if laneCredentials == "" {
			laneCredentials = gcPath
		}
		laneFolderID := lane.GoogleFolderID
		if laneFolderID == "" {
			laneFolderID = appCfg.GoogleFolderID
		}
		if laneFolderID == "" {
			return nil, fmt.Errorf("google_lanes[%d] is missing google_folder_id", idx)
		}
		laneTransport := lane.Transport
		if laneTransport.TargetIP == "" && laneTransport.SNI == "" && laneTransport.HostHeader == "" {
			laneTransport = appCfg.Transport
		}
		googleBackend := storage.NewGoogleBackend(httpclient.NewCustomClient(laneTransport), laneCredentials, laneFolderID)
		googleBackend.SetRetryPolicy(appCfg.StorageRetryMax, appCfg.StorageRetryBaseMs)
		backends = append(backends, googleBackend)
	}
	return storage.NewMultiBackend(backends)
}
