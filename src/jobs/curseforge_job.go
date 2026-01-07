package jobs

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/google/uuid"

	"github.com/pyrohost/elytra/src/config"
	"github.com/pyrohost/elytra/src/internal/ufs"
	"github.com/pyrohost/elytra/src/server"
	"github.com/pyrohost/elytra/src/system"
)

const (
	curseforgeJobTypeDownload     = "curseforge_download"
	curseforgeJobTypeDownloadBulk = "curseforge_download_bulk"
	curseforgeJobTypeUpdate        = "curseforge_update"
	curseforgeJobTypeRemove        = "curseforge_remove"
	curseforgeJobTypeScan          = "curseforge_scan"
	curseforgeJobTypeUploadBlocked = "curseforge_upload_blocked"

	curseforgeMaxFileSizeBytes         = int64(100 * 1024 * 1024)
	curseforgeItemProgressDownloadBase = 10
	curseforgeItemProgressDownloadSpan = 75
	curseforgeItemProgressValidate     = 90
	curseforgeItemProgressFinalize     = 95

	curseforgeAPIBaseURL = "https://api.curseforge.com/v1"
	curseforgeGameID     = 432 // Minecraft
)

var (
	curseforgeAllowedDirectories = map[string]struct{}{
		"/mods":    {},
		"/plugins": {},
	}
	curseforgeRemovableDirectories = map[string]struct{}{
		"/mods":    {},
		"/plugins": {},
		"/config":  {},
	}
	curseforgeAllowedHosts = map[string]struct{}{
		"edge.forgecdn.net":       {}, // Primary CDN
		"mediafilez.forgecdn.net": {}, // Secondary CDN
		"www.curseforge.com":      {}, // Direct downloads only
	}

	curseforgeDownloadClient = newCurseForgeHTTPClient()

	curseforgeBlockedRanges = []*net.IPNet{
		mustParseCIDR("127.0.0.1/8"),
		mustParseCIDR("10.0.0.0/8"),
		mustParseCIDR("172.16.0.0/12"),
		mustParseCIDR("192.168.0.0/16"),
		mustParseCIDR("169.254.0.0/16"),
		mustParseCIDR("::1/128"),
		mustParseCIDR("fe80::/10"),
		mustParseCIDR("fc00::/7"),
	}
)

type CurseForgeDownloadJob struct {
	id            string
	serverID      string
	modID         int
	fileID        int
	fileURL       *url.URL
	fileName      string
	fileHashSHA1  []byte
	fileHashHex   string
	directory     string
	overwrite     bool
	maxFileSize   int64
	isBlocked     bool
	apiKey        string
	progress      int
	message       string
	serverManager *server.Manager
}

type curseforgeDownloadSpec struct {
	modID        int
	fileID       int
	fileURL      *url.URL
	fileName     string
	fileHashSHA1 []byte
	fileHashHex  string
	directory    string
	overwrite    bool
	maxFileSize  int64
	isBlocked    bool
}

type CurseForgeBulkDownloadJob struct {
	id            string
	serverID      string
	downloads     []curseforgeDownloadSpec
	apiKey        string
	progress      int
	message       string
	serverManager *server.Manager
}

type curseforgeUpdateSpec struct {
	download    curseforgeDownloadSpec
	oldFilePath string
}

type CurseForgeUpdateJob struct {
	id             string
	serverID       string
	updates        []curseforgeUpdateSpec
	deleteOldFiles bool
	apiKey         string
	progress       int
	message        string
	serverManager  *server.Manager
}

type CurseForgeScanJob struct {
	id            string
	serverID      string
	directories   []string
	progress      int
	message       string
	serverManager *server.Manager
}

type CurseForgeRemoveJob struct {
	id            string
	serverID      string
	filePath      string
	progress      int
	message       string
	serverManager *server.Manager
}

type CurseForgeUploadBlockedJob struct {
	id            string
	serverID      string
	fileName      string
	fileContents  []byte
	fileHashSHA1  []byte
	fileHashHex   string
	directory     string
	maxFileSize   int64
	progress      int
	message       string
	serverManager *server.Manager
}

func NewCurseForgeDownloadJob(data map[string]interface{}, serverManager *server.Manager) (Job, error) {
	serverID, err := requireStringField(data, "server_id")
	if err != nil {
		return nil, err
	}

	requestMax, err := optionalInt64Field(data, "max_file_size")
	if err != nil {
		return nil, err
	}

	spec, err := parseCurseForgeDownloadSpec(data, requestMax, false)
	if err != nil {
		return nil, err
	}

	apiKey, _ := optionalStringField(data, "api_key")

	return &CurseForgeDownloadJob{
		id:            uuid.New().String(),
		serverID:      serverID,
		modID:         spec.modID,
		fileID:         spec.fileID,
		fileURL:       spec.fileURL,
		fileName:      spec.fileName,
		fileHashSHA1:  spec.fileHashSHA1,
		fileHashHex:   spec.fileHashHex,
		directory:     spec.directory,
		overwrite:     spec.overwrite,
		maxFileSize:   spec.maxFileSize,
		isBlocked:     spec.isBlocked,
		apiKey:        apiKey,
		progress:      0,
		message:       "CurseForge download queued",
		serverManager: serverManager,
	}, nil
}

func NewCurseForgeScanJob(data map[string]interface{}, serverManager *server.Manager) (Job, error) {
	serverID, err := requireStringField(data, "server_id")
	if err != nil {
		return nil, err
	}

	dirs, err := optionalStringSliceField(data, "directories")
	if err != nil {
		return nil, err
	}
	directories, err := normalizeCurseForgeDirectories(dirs)
	if err != nil {
		return nil, err
	}

	return &CurseForgeScanJob{
		id:            uuid.New().String(),
		serverID:      serverID,
		directories:   directories,
		progress:      0,
		message:       "CurseForge scan queued",
		serverManager: serverManager,
	}, nil
}

func NewCurseForgeBulkDownloadJob(data map[string]interface{}, serverManager *server.Manager) (Job, error) {
	serverID, err := requireStringField(data, "server_id")
	if err != nil {
		return nil, err
	}

	requestMax, err := optionalInt64Field(data, "max_file_size")
	if err != nil {
		return nil, err
	}

	items, err := requireMapSliceField(data, "downloads")
	if err != nil {
		return nil, err
	}

	downloads := make([]curseforgeDownloadSpec, 0, len(items))
	for _, item := range items {
		spec, err := parseCurseForgeDownloadSpec(item, requestMax, false)
		if err != nil {
			return nil, err
		}
		downloads = append(downloads, spec)
	}

	if len(downloads) == 0 {
		return nil, fmt.Errorf("downloads are required")
	}

	apiKey, _ := optionalStringField(data, "api_key")

	return &CurseForgeBulkDownloadJob{
		id:            uuid.New().String(),
		serverID:      serverID,
		downloads:     downloads,
		apiKey:        apiKey,
		progress:      0,
		message:       "CurseForge bulk download queued",
		serverManager: serverManager,
	}, nil
}

func NewCurseForgeUpdateJob(data map[string]interface{}, serverManager *server.Manager) (Job, error) {
	serverID, err := requireStringField(data, "server_id")
	if err != nil {
		return nil, err
	}

	requestMax, err := optionalInt64Field(data, "max_file_size")
	if err != nil {
		return nil, err
	}

	items, err := requireMapSliceField(data, "downloads")
	if err != nil {
		return nil, err
	}

	deleteOldFiles, _ := optionalBoolField(data, "delete_old_files")

	updates := make([]curseforgeUpdateSpec, 0, len(items))
	for _, item := range items {
		spec, err := parseCurseForgeDownloadSpec(item, requestMax, true)
		if err != nil {
			return nil, err
		}

		oldPathRaw, err := requireStringField(item, "old_file_path")
		if err != nil {
			return nil, err
		}
		oldPath, err := normalizeCurseForgeFilePath(oldPathRaw, curseforgeAllowedDirectories)
		if err != nil {
			return nil, err
		}

		updates = append(updates, curseforgeUpdateSpec{
			download:    spec,
			oldFilePath: oldPath,
		})
	}

	if len(updates) == 0 {
		return nil, fmt.Errorf("downloads are required")
	}

	apiKey, _ := optionalStringField(data, "api_key")

	return &CurseForgeUpdateJob{
		id:             uuid.New().String(),
		serverID:       serverID,
		updates:        updates,
		deleteOldFiles: deleteOldFiles,
		apiKey:         apiKey,
		progress:       0,
		message:        "CurseForge update queued",
		serverManager:  serverManager,
	}, nil
}

func NewCurseForgeRemoveJob(data map[string]interface{}, serverManager *server.Manager) (Job, error) {
	serverID, err := requireStringField(data, "server_id")
	if err != nil {
		return nil, err
	}

	filePathRaw, err := requireStringField(data, "file_path")
	if err != nil {
		return nil, err
	}
	filePath, err := normalizeCurseForgeFilePath(filePathRaw, curseforgeRemovableDirectories)
	if err != nil {
		return nil, err
	}

	return &CurseForgeRemoveJob{
		id:            uuid.New().String(),
		serverID:      serverID,
		filePath:      filePath,
		progress:      0,
		message:       "CurseForge remove queued",
		serverManager: serverManager,
	}, nil
}

func NewCurseForgeUploadBlockedJob(data map[string]interface{}, serverManager *server.Manager) (Job, error) {
	serverID, err := requireStringField(data, "server_id")
	if err != nil {
		return nil, err
	}

	requestMax, err := optionalInt64Field(data, "max_file_size")
	if err != nil {
		return nil, err
	}

	fileNameRaw, err := requireStringField(data, "file_name")
	if err != nil {
		return nil, err
	}
	fileName, err := normalizeCurseForgeFileName(fileNameRaw)
	if err != nil {
		return nil, err
	}

	fileContentsRaw, err := requireStringField(data, "file_contents")
	if err != nil {
		return nil, err
	}

	// Decode base64 file contents
	fileContents, err := base64.StdEncoding.DecodeString(fileContentsRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 file_contents: %w", err)
	}

	fileHashRaw, err := requireStringField(data, "file_hash")
	if err != nil {
		return nil, err
	}
	hashBytes, hashHex, err := parseCurseForgeSHA1(fileHashRaw)
	if err != nil {
		return nil, err
	}

	directoryRaw, err := requireStringField(data, "directory")
	if err != nil {
		return nil, err
	}
	directory, err := normalizeCurseForgeDirectory(directoryRaw)
	if err != nil {
		return nil, err
	}

	maxSize := resolveCurseForgeMaxFileSize(requestMax)
	if int64(len(fileContents)) > maxSize {
		return nil, fmt.Errorf("file exceeds maximum size of %s", system.FormatBytes(maxSize))
	}

	return &CurseForgeUploadBlockedJob{
		id:            uuid.New().String(),
		serverID:      serverID,
		fileName:      fileName,
		fileContents:  fileContents,
		fileHashSHA1:  hashBytes,
		fileHashHex:   hashHex,
		directory:     directory,
		maxFileSize:   maxSize,
		progress:      0,
		message:       "CurseForge upload blocked mod queued",
		serverManager: serverManager,
	}, nil
}

func (j *CurseForgeDownloadJob) GetID() string      { return j.id }
func (j *CurseForgeDownloadJob) GetType() string    { return curseforgeJobTypeDownload }
func (j *CurseForgeDownloadJob) GetProgress() int   { return j.progress }
func (j *CurseForgeDownloadJob) GetMessage() string { return j.message }
func (j *CurseForgeDownloadJob) IsAsync() bool      { return true }

func (j *CurseForgeDownloadJob) GetServerID() string           { return j.serverID }
func (j *CurseForgeDownloadJob) GetWebSocketEventType() string { return "curseforge.status" }
func (j *CurseForgeDownloadJob) GetWebSocketEventData() map[string]interface{} {
	return map[string]interface{}{
		"operation": "download",
		"mod_id":    j.modID,
		"file_id":   j.fileID,
		"file_name": j.fileName,
		"directory": j.directory,
		"blocked":   j.isBlocked,
	}
}

func (j *CurseForgeDownloadJob) Validate() error {
	if j.serverID == "" {
		return fmt.Errorf("server_id is required")
	}
	// Don't fail validation for blocked mods - let Execute handle it with proper response
	if j.fileName == "" {
		return fmt.Errorf("file_name is required")
	}
	if j.directory == "" {
		return fmt.Errorf("directory is required")
	}
	// If blocked, file_url and file_hash may be empty/null
	if !j.isBlocked {
		if j.fileURL == nil {
			return fmt.Errorf("file_url is required")
		}
		if len(j.fileHashSHA1) == 0 {
			return fmt.Errorf("file_hash is required")
		}
	}
	return nil
}

func (j *CurseForgeDownloadJob) Execute(ctx context.Context, reporter ProgressReporter) (interface{}, error) {
	logger := log.WithFields(log.Fields{
		"job_id":    j.id,
		"server_id": j.serverID,
		"file":      j.fileName,
	})

	if config.Get().Api.DisableRemoteDownload {
		return nil, errors.New("remote downloads are disabled on this node")
	}

	reporter.ReportProgress(5, "Locating server...")
	s, exists := j.serverManager.Get(j.serverID)
	if !exists {
		return nil, errors.New("server not found: " + j.serverID)
	}

	targetPath := path.Join(j.directory, j.fileName)
	if err := s.Filesystem().IsIgnored(targetPath); err != nil {
		return nil, err
	}

	logger.WithField("target", targetPath).Info("starting curseforge download")

	// Handle blocked mods with special response
	if j.isBlocked {
		reporter.ReportProgress(100, "Mod distribution is blocked")
		return map[string]interface{}{
			"successful":        false,
			"job_type":          curseforgeJobTypeDownload,
			"status":            "blocked",
			"message":           "This mod requires manual download",
			"download_page_url": fmt.Sprintf("https://www.curseforge.com/minecraft/mc-mods/%d/files/%d", j.modID, j.fileID),
			"expected_file": map[string]interface{}{
				"name": j.fileName,
				"size": 0, // Size not available for blocked mods
				"sha1": j.fileHashHex,
			},
		}, nil
	}

	reporter.ReportProgress(10, "Preparing download...")
	result, err := j.downloadToServer(ctx, reporter, s, targetPath, j.maxFileSize)
	if err != nil {
		logger.WithError(err).Error("curseforge download failed")
		return nil, err
	}

	logger.Info("curseforge download completed successfully")
	return result, nil
}

func (j *CurseForgeDownloadJob) downloadToServer(ctx context.Context, reporter ProgressReporter, s *server.Server, targetPath string, maxFileSize int64) (map[string]interface{}, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.fileURL.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create download request")
	}
	req.Header.Set("User-Agent", "Elytra Daemon (https://pyrodactyl.dev)")

	resp, err := curseforgeDownloadClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to download file")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected download response: %d %s", resp.StatusCode, resp.Status)
	}

	contentLength := resp.ContentLength
	if contentLength < 1 {
		return nil, errors.New("download response missing content length")
	}

	maxSize := resolveCurseForgeMaxFileSize(maxFileSize)
	if contentLength > maxSize {
		return nil, errors.Errorf("file exceeds maximum size of %s", system.FormatBytes(maxSize))
	}

	tempPath := path.Join(j.directory, fmt.Sprintf(".elytra-curseforge-%s-%s", uuid.New().String(), j.fileName))
	if err := s.Filesystem().IsIgnored(tempPath); err != nil {
		return nil, err
	}

	reporter.ReportProgress(curseforgeItemProgressDownloadBase, "Downloading file...")

	hasher := sha1.New()
	progressReader := newCurseForgeProgressReader(
		io.TeeReader(resp.Body, hasher),
		contentLength,
		j.fileName,
		reporter,
		curseforgeItemProgressDownloadBase,
		curseforgeItemProgressDownloadSpan,
	)

	if err := s.Filesystem().Write(tempPath, progressReader, contentLength, 0o644); err != nil {
		_ = s.Filesystem().Delete(tempPath)
		return nil, errors.Wrap(err, "failed to write downloaded file")
	}

	if progressReader.BytesRead() < contentLength {
		_ = s.Filesystem().Delete(tempPath)
		return nil, errors.New("download incomplete before expected length")
	}

	actualHash := hasher.Sum(nil)
	if !bytes.Equal(actualHash, j.fileHashSHA1) {
		_ = s.Filesystem().Delete(tempPath)
		return nil, fmt.Errorf("file integrity check failed - expected SHA-1 %s, got %s", j.fileHashHex, hex.EncodeToString(actualHash))
	}

	reporter.ReportProgress(curseforgeItemProgressValidate, "Validating download...")

	if err := j.replaceTargetFile(s, tempPath, targetPath); err != nil {
		_ = s.Filesystem().Delete(tempPath)
		return nil, err
	}

	reporter.ReportProgress(curseforgeItemProgressFinalize, "Finalizing download...")

	return map[string]interface{}{
		"successful": true,
		"job_type":   curseforgeJobTypeDownload,
		"file_path":  targetPath,
		"file_size":  contentLength,
		"file_hash":  "sha1:" + hex.EncodeToString(actualHash),
	}, nil
}

func (j *CurseForgeDownloadJob) replaceTargetFile(s *server.Server, tempPath, targetPath string) error {
	if st, err := s.Filesystem().Stat(targetPath); err == nil {
		if st.IsDir() {
			return errors.New("target path is a directory")
		}
		if !j.overwrite {
			return errors.New("target file already exists")
		}
		if err := s.Filesystem().Delete(targetPath); err != nil {
			return errors.Wrap(err, "failed to remove existing file")
		}
	} else if !errors.Is(err, ufs.ErrNotExist) {
		return errors.Wrap(err, "failed to check target file")
	}

	if err := s.Filesystem().Rename(tempPath, targetPath); err != nil {
		return errors.Wrap(err, "failed to move downloaded file into place")
	}
	return nil
}

func (j *CurseForgeBulkDownloadJob) GetID() string      { return j.id }
func (j *CurseForgeBulkDownloadJob) GetType() string    { return curseforgeJobTypeDownloadBulk }
func (j *CurseForgeBulkDownloadJob) GetProgress() int   { return j.progress }
func (j *CurseForgeBulkDownloadJob) GetMessage() string { return j.message }
func (j *CurseForgeBulkDownloadJob) IsAsync() bool      { return true }

func (j *CurseForgeBulkDownloadJob) GetServerID() string           { return j.serverID }
func (j *CurseForgeBulkDownloadJob) GetWebSocketEventType() string { return "curseforge.status" }
func (j *CurseForgeBulkDownloadJob) GetWebSocketEventData() map[string]interface{} {
	return map[string]interface{}{
		"operation": "download_bulk",
		"count":     len(j.downloads),
	}
}

func (j *CurseForgeBulkDownloadJob) Validate() error {
	if j.serverID == "" {
		return fmt.Errorf("server_id is required")
	}
	if len(j.downloads) == 0 {
		return fmt.Errorf("downloads are required")
	}
	return nil
}

func (j *CurseForgeBulkDownloadJob) Execute(ctx context.Context, reporter ProgressReporter) (interface{}, error) {
	logger := log.WithFields(log.Fields{
		"job_id":    j.id,
		"server_id": j.serverID,
		"count":     len(j.downloads),
	})

	if config.Get().Api.DisableRemoteDownload {
		return nil, errors.New("remote downloads are disabled on this node")
	}

	reporter.ReportProgress(5, "Locating server...")
	s, exists := j.serverManager.Get(j.serverID)
	if !exists {
		return nil, errors.New("server not found: " + j.serverID)
	}

	downloaded := make([]map[string]interface{}, 0, len(j.downloads))
	failed := make([]map[string]interface{}, 0)

	for i, download := range j.downloads {
		if download.isBlocked {
			failed = append(failed, map[string]interface{}{
				"file_name":    download.fileName,
				"error_message": "mod distribution is blocked, manual download required",
			})
			continue
		}

		itemBase, itemSpan := curseforgeItemRange(i, len(j.downloads), 10, 80)
		itemReporter := newCurseForgeScaledReporter(reporter, itemBase, itemSpan)

		itemReporter.ReportProgress(0, fmt.Sprintf("Downloading %d of %d...", i+1, len(j.downloads)))

		result, err := downloadCurseForgeSpec(ctx, itemReporter, s, download)
		if err != nil {
			logger.WithField("file", download.fileName).WithError(err).Warn("curseforge bulk download failed")
			failed = append(failed, map[string]interface{}{
				"file_name":    download.fileName,
				"error_message": err.Error(),
			})
			continue
		}

		entry := map[string]interface{}{
			"file_path": result["file_path"],
			"file_size": result["file_size"],
			"file_hash": result["file_hash"],
		}
		downloaded = append(downloaded, entry)
	}

	reporter.ReportProgress(95, "Finalizing downloads...")

	successful := len(failed) == 0
	return map[string]interface{}{
		"successful":       successful,
		"job_type":         curseforgeJobTypeDownloadBulk,
		"downloaded_count": len(downloaded),
		"failed_count":     len(failed),
		"files":            downloaded,
		"failed_downloads": failed,
	}, nil
}

func (j *CurseForgeUpdateJob) GetID() string      { return j.id }
func (j *CurseForgeUpdateJob) GetType() string    { return curseforgeJobTypeUpdate }
func (j *CurseForgeUpdateJob) GetProgress() int   { return j.progress }
func (j *CurseForgeUpdateJob) GetMessage() string { return j.message }
func (j *CurseForgeUpdateJob) IsAsync() bool      { return true }

func (j *CurseForgeUpdateJob) GetServerID() string           { return j.serverID }
func (j *CurseForgeUpdateJob) GetWebSocketEventType() string { return "curseforge.status" }
func (j *CurseForgeUpdateJob) GetWebSocketEventData() map[string]interface{} {
	return map[string]interface{}{
		"operation":        "update",
		"count":            len(j.updates),
		"delete_old_files": j.deleteOldFiles,
	}
}

func (j *CurseForgeUpdateJob) Validate() error {
	if j.serverID == "" {
		return fmt.Errorf("server_id is required")
	}
	if len(j.updates) == 0 {
		return fmt.Errorf("downloads are required")
	}
	return nil
}

func (j *CurseForgeUpdateJob) Execute(ctx context.Context, reporter ProgressReporter) (interface{}, error) {
	logger := log.WithFields(log.Fields{
		"job_id":    j.id,
		"server_id": j.serverID,
		"count":     len(j.updates),
	})

	if config.Get().Api.DisableRemoteDownload {
		return nil, errors.New("remote downloads are disabled on this node")
	}

	reporter.ReportProgress(5, "Locating server...")
	s, exists := j.serverManager.Get(j.serverID)
	if !exists {
		return nil, errors.New("server not found: " + j.serverID)
	}

	updated := 0
	deleted := make([]string, 0)
	failed := make([]map[string]interface{}, 0)
	deleteFailures := make([]map[string]interface{}, 0)
	files := make([]map[string]interface{}, 0)

	for i, update := range j.updates {
		if update.download.isBlocked {
			failed = append(failed, map[string]interface{}{
				"mod_id":    update.download.modID,
				"file_id":   update.download.fileID,
				"file_name": update.download.fileName,
				"error":     "mod distribution is blocked, manual download required",
			})
			continue
		}

		itemBase, itemSpan := curseforgeItemRange(i, len(j.updates), 10, 80)
		itemReporter := newCurseForgeScaledReporter(reporter, itemBase, itemSpan)

		itemReporter.ReportProgress(0, fmt.Sprintf("Updating %d of %d...", i+1, len(j.updates)))

		result, err := downloadCurseForgeSpec(ctx, itemReporter, s, update.download)
		if err != nil {
			logger.WithField("file", update.download.fileName).WithError(err).Warn("curseforge update download failed")
			failed = append(failed, map[string]interface{}{
				"mod_id":    update.download.modID,
				"file_id":   update.download.fileID,
				"file_name": update.download.fileName,
				"error":     err.Error(),
			})
			continue
		}

		updated++

		// Track updated file info
		fileInfo := map[string]interface{}{
			"file_path": result["file_path"],
			"file_size": result["file_size"],
			"file_hash": result["file_hash"],
			"old_file_path": update.oldFilePath,
		}

		if j.deleteOldFiles {
			targetPath, _ := result["file_path"].(string)
			if targetPath != "" && targetPath != update.oldFilePath {
				deletedFile, err := deleteCurseForgeFileIfExists(s, update.oldFilePath)
				if err != nil {
					logger.WithField("old_path", update.oldFilePath).WithError(err).Warn("failed to remove old mod file")
					deleteFailures = append(deleteFailures, map[string]interface{}{
						"file_path": update.oldFilePath,
						"error":     err.Error(),
					})
					fileInfo["old_file_deleted"] = false
				} else if deletedFile {
					deleted = append(deleted, update.oldFilePath)
					fileInfo["old_file_deleted"] = true
				} else {
					fileInfo["old_file_deleted"] = false
				}
			} else {
				fileInfo["old_file_deleted"] = false
			}
		} else {
			fileInfo["old_file_deleted"] = false
		}

		// Store file info for response
		files = append(files, fileInfo)
	}

	reporter.ReportProgress(95, "Finalizing updates...")

	successful := len(failed) == 0 && len(deleteFailures) == 0
	return map[string]interface{}{
		"successful":        successful,
		"job_type":          curseforgeJobTypeUpdate,
		"updated_count":     updated,
		"failed_count":      len(failed),
		"files":             files,
		"failed_updates":    failed,
		"deleted_old_files": deleted,
		"delete_failures":   deleteFailures,
	}, nil
}

func (j *CurseForgeRemoveJob) GetID() string      { return j.id }
func (j *CurseForgeRemoveJob) GetType() string    { return curseforgeJobTypeRemove }
func (j *CurseForgeRemoveJob) GetProgress() int   { return j.progress }
func (j *CurseForgeRemoveJob) GetMessage() string { return j.message }

func (j *CurseForgeRemoveJob) GetServerID() string           { return j.serverID }
func (j *CurseForgeRemoveJob) GetWebSocketEventType() string { return "curseforge.status" }
func (j *CurseForgeRemoveJob) GetWebSocketEventData() map[string]interface{} {
	return map[string]interface{}{
		"operation": "remove",
		"file_path": j.filePath,
	}
}

func (j *CurseForgeRemoveJob) Validate() error {
	if j.serverID == "" {
		return fmt.Errorf("server_id is required")
	}
	if j.filePath == "" {
		return fmt.Errorf("file_path is required")
	}
	return nil
}

func (j *CurseForgeRemoveJob) Execute(ctx context.Context, reporter ProgressReporter) (interface{}, error) {
	logger := log.WithFields(log.Fields{
		"job_id":    j.id,
		"server_id": j.serverID,
		"file":      j.filePath,
	})

	reporter.ReportProgress(5, "Locating server...")
	s, exists := j.serverManager.Get(j.serverID)
	if !exists {
		return nil, errors.New("server not found: " + j.serverID)
	}

	reporter.ReportProgress(50, "Removing file...")
	deleted, err := deleteCurseForgeFileIfExists(s, j.filePath)
	if err != nil {
		logger.WithError(err).Error("curseforge remove failed")
		return nil, err
	}

	message := "File removed"
	if !deleted {
		message = "File already removed"
	}
	reporter.ReportProgress(90, message)

	return map[string]interface{}{
		"successful": true,
		"job_type":   curseforgeJobTypeRemove,
		"file_path":  j.filePath,
		"message":    "File removed successfully",
	}, nil
}

func (j *CurseForgeUploadBlockedJob) GetID() string      { return j.id }
func (j *CurseForgeUploadBlockedJob) GetType() string    { return curseforgeJobTypeUploadBlocked }
func (j *CurseForgeUploadBlockedJob) GetProgress() int   { return j.progress }
func (j *CurseForgeUploadBlockedJob) GetMessage() string { return j.message }
func (j *CurseForgeUploadBlockedJob) IsAsync() bool      { return true }

func (j *CurseForgeUploadBlockedJob) GetServerID() string           { return j.serverID }
func (j *CurseForgeUploadBlockedJob) GetWebSocketEventType() string { return "curseforge.status" }
func (j *CurseForgeUploadBlockedJob) GetWebSocketEventData() map[string]interface{} {
	return map[string]interface{}{
		"operation": "upload_blocked",
		"file_name": j.fileName,
		"directory": j.directory,
	}
}

func (j *CurseForgeUploadBlockedJob) Validate() error {
	if j.serverID == "" {
		return fmt.Errorf("server_id is required")
	}
	if len(j.fileContents) == 0 {
		return fmt.Errorf("file_contents is required")
	}
	if len(j.fileContents) > int(j.maxFileSize) {
		return fmt.Errorf("file exceeds maximum size of %s", system.FormatBytes(j.maxFileSize))
	}
	if len(j.fileHashSHA1) == 0 {
		return fmt.Errorf("file_hash is required")
	}
	if j.fileName == "" {
		return fmt.Errorf("file_name is required")
	}
	if j.directory == "" {
		return fmt.Errorf("directory is required")
	}
	return nil
}

func (j *CurseForgeUploadBlockedJob) Execute(ctx context.Context, reporter ProgressReporter) (interface{}, error) {
	logger := log.WithFields(log.Fields{
		"job_id":    j.id,
		"server_id": j.serverID,
		"file":      j.fileName,
	})

	reporter.ReportProgress(5, "Locating server...")
	s, exists := j.serverManager.Get(j.serverID)
	if !exists {
		return nil, errors.New("server not found: " + j.serverID)
	}

	targetPath := path.Join(j.directory, j.fileName)
	if err := s.Filesystem().IsIgnored(targetPath); err != nil {
		return nil, err
	}

	logger.WithField("target", targetPath).Info("starting curseforge upload blocked mod")

	reporter.ReportProgress(20, "Validating file hash...")

	// Calculate SHA-1 hash of file contents
	hasher := sha1.New()
	hasher.Write(j.fileContents)
	actualHash := hasher.Sum(nil)

	if !bytes.Equal(actualHash, j.fileHashSHA1) {
		return nil, fmt.Errorf("file integrity check failed - expected SHA-1 %s, got %s", j.fileHashHex, hex.EncodeToString(actualHash))
	}

	reporter.ReportProgress(50, "Writing file to server...")

	// Write file to target path
	if err := s.Filesystem().Write(targetPath, bytes.NewReader(j.fileContents), int64(len(j.fileContents)), 0o644); err != nil {
		return nil, errors.Wrap(err, "failed to write file")
	}

	reporter.ReportProgress(95, "Finalizing upload...")

	logger.Info("curseforge upload blocked mod completed successfully")

	return map[string]interface{}{
		"successful": true,
		"job_type":   curseforgeJobTypeUploadBlocked,
		"file_path":  targetPath,
		"file_size":  len(j.fileContents),
		"file_hash":  "sha1:" + hex.EncodeToString(actualHash),
	}, nil
}

func (j *CurseForgeScanJob) GetID() string      { return j.id }
func (j *CurseForgeScanJob) GetType() string    { return curseforgeJobTypeScan }
func (j *CurseForgeScanJob) GetProgress() int   { return j.progress }
func (j *CurseForgeScanJob) GetMessage() string { return j.message }

func (j *CurseForgeScanJob) GetServerID() string           { return j.serverID }
func (j *CurseForgeScanJob) GetWebSocketEventType() string { return "curseforge.status" }
func (j *CurseForgeScanJob) GetWebSocketEventData() map[string]interface{} {
	return map[string]interface{}{
		"operation":   "scan",
		"directories": j.directories,
	}
}

func (j *CurseForgeScanJob) Validate() error {
	if j.serverID == "" {
		return fmt.Errorf("server_id is required")
	}
	if len(j.directories) == 0 {
		return fmt.Errorf("directories are required")
	}
	return nil
}

func (j *CurseForgeScanJob) Execute(ctx context.Context, reporter ProgressReporter) (interface{}, error) {
	logger := log.WithFields(log.Fields{
		"job_id":    j.id,
		"server_id": j.serverID,
	})

	reporter.ReportProgress(5, "Locating server...")
	s, exists := j.serverManager.Get(j.serverID)
	if !exists {
		return nil, errors.New("server not found: " + j.serverID)
	}

	reporter.ReportProgress(10, "Scanning mod directories...")
	entries, err := j.collectJarEntries(s)
	if err != nil {
		logger.WithError(err).Error("curseforge scan failed")
		return nil, err
	}

	if len(entries) == 0 {
		reporter.ReportProgress(90, "No mods detected")
		return map[string]interface{}{
			"successful": true,
			"job_type":   curseforgeJobTypeScan,
			"files":      []map[string]interface{}{},
		}, nil
	}

	reporter.ReportProgress(20, fmt.Sprintf("Found %d files, hashing...", len(entries)))

	files := make([]map[string]interface{}, 0, len(entries))
	for i, entry := range entries {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		sha1Hex, err := hashFileSHA1(s, entry.path)
		if err != nil {
			return nil, err
		}

		sha512Hex, err := hashFileSHA512(s, entry.path)
		if err != nil {
			return nil, err
		}

		murmur2, err := hashFileMurmur2(s, entry.path)
		if err != nil {
			return nil, err
		}

		progress := 20 + ((i + 1) * 60 / len(entries))
		reporter.ReportProgress(progress, fmt.Sprintf("Hashed %s (%d/%d)", entry.name, i+1, len(entries)))

		files = append(files, map[string]interface{}{
			"path":     entry.path,
			"name":     entry.name,
			"size":     entry.size,
			"hash":     "sha512:" + sha512Hex, // SHA-512 for backward compatibility
			"hashes": map[string]interface{}{
				"sha1":    sha1Hex,
				"sha512":  sha512Hex,
				"murmur2": murmur2,
			},
			"modified": entry.modified,
		})
	}

	reporter.ReportProgress(95, "Finalizing scan results...")

	return map[string]interface{}{
		"successful": true,
		"job_type":   curseforgeJobTypeScan,
		"files":       files,
	}, nil
}

type curseforgeJarEntry struct {
	path     string
	name     string
	size     int64
	modified string
}

func (j *CurseForgeScanJob) collectJarEntries(s *server.Server) ([]curseforgeJarEntry, error) {
	var entries []curseforgeJarEntry
	fs := s.Filesystem().UnixFS()
	for _, dir := range j.directories {
		err := ufs.WalkDir(fs, dir, func(p string, d ufs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, ufs.ErrNotExist) {
					return ufs.SkipDir
				}
				return err
			}
			if d.IsDir() {
				return nil
			}
			if !d.Type().IsRegular() {
				return nil
			}
			if !strings.HasSuffix(strings.ToLower(d.Name()), ".jar") {
				return nil
			}
			info, err := fs.Stat(p)
			if err != nil {
				return err
			}
			entries = append(entries, curseforgeJarEntry{
				path:     p,
				name:     info.Name(),
				size:     info.Size(),
				modified: info.ModTime().UTC().Format(time.RFC3339),
			})
			return nil
		})
		if err != nil && !errors.Is(err, ufs.SkipDir) {
			return nil, errors.Wrap(err, "failed to scan mod directory")
		}
	}
	return entries, nil
}

type curseforgeProgressReader struct {
	reader      io.Reader
	total       int64
	read        int64
	fileName    string
	reporter    ProgressReporter
	lastPercent int
	base        int
	span        int
}

func newCurseForgeProgressReader(reader io.Reader, total int64, fileName string, reporter ProgressReporter, base, span int) *curseforgeProgressReader {
	return &curseforgeProgressReader{
		reader:   reader,
		total:     total,
		fileName:  fileName,
		reporter: reporter,
		base:     base,
		span:     span,
	}
}

func (p *curseforgeProgressReader) Read(b []byte) (int, error) {
	n, err := p.reader.Read(b)
	if n > 0 {
		p.read += int64(n)
		p.report()
	}
	return n, err
}

func (p *curseforgeProgressReader) BytesRead() int64 {
	return p.read
}

func (p *curseforgeProgressReader) report() {
	if p.total <= 0 {
		return
	}
	percent := int((p.read * 100) / p.total)
	if percent <= p.lastPercent {
		return
	}
	p.lastPercent = percent

	progress := p.base + (percent*p.span)/100
	if progress > p.base+p.span {
		progress = p.base + p.span
	}

	message := fmt.Sprintf(
		"Downloading %s (%s/%s)",
		p.fileName,
		system.FormatBytes(p.read),
		system.FormatBytes(p.total),
	)

	p.reporter.ReportProgress(progress, message)
}

type curseforgeScaledReporter struct {
	base     int
	span     int
	reporter ProgressReporter
}

func newCurseForgeScaledReporter(reporter ProgressReporter, base, span int) *curseforgeScaledReporter {
	return &curseforgeScaledReporter{
		base:     base,
		span:     span,
		reporter: reporter,
	}
}

func (r *curseforgeScaledReporter) ReportProgress(progress int, message string) {
	if progress < 0 {
		progress = 0
	}
	if progress > 100 {
		progress = 100
	}
	scaled := r.base + (progress*r.span)/100
	if scaled > 100 {
		scaled = 100
	}
	r.reporter.ReportProgress(scaled, message)
}

func (r *curseforgeScaledReporter) ReportStatus(status Status, message string) {
	r.reporter.ReportStatus(status, message)
}

func curseforgeItemRange(index, total, base, span int) (int, int) {
	if total <= 0 {
		return base, span
	}
	start := base + (index*span)/total
	end := base + ((index+1)*span)/total
	if end < start {
		end = start
	}
	return start, end - start
}

func downloadCurseForgeSpec(ctx context.Context, reporter ProgressReporter, s *server.Server, spec curseforgeDownloadSpec) (map[string]interface{}, error) {
	targetPath := path.Join(spec.directory, spec.fileName)
	if err := s.Filesystem().IsIgnored(targetPath); err != nil {
		return nil, err
	}

	job := &CurseForgeDownloadJob{
		fileURL:      spec.fileURL,
		fileName:     spec.fileName,
		fileHashSHA1: spec.fileHashSHA1,
		fileHashHex:  spec.fileHashHex,
		directory:    spec.directory,
		overwrite:    spec.overwrite,
		isBlocked:    spec.isBlocked,
	}

	return job.downloadToServer(ctx, reporter, s, targetPath, spec.maxFileSize)
}

func parseCurseForgeURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || !u.IsAbs() {
		return nil, fmt.Errorf("invalid file_url")
	}
	if u.Scheme != "https" {
		return nil, fmt.Errorf("file_url must use https")
	}
	host := strings.ToLower(u.Hostname())
	if _, ok := curseforgeAllowedHosts[host]; !ok {
		return nil, fmt.Errorf("file_url host is not allowed")
	}
	return u, nil
}

func normalizeCurseForgeDirectory(dir string) (string, error) {
	cleaned := path.Clean("/" + strings.TrimSpace(dir))
	if cleaned == "." || cleaned == "/" {
		return "", fmt.Errorf("directory is required")
	}
	if _, ok := curseforgeAllowedDirectories[cleaned]; !ok {
		return "", fmt.Errorf("directory must be /mods or /plugins")
	}
	return cleaned, nil
}

func normalizeCurseForgeDirectories(dirs []string) ([]string, error) {
	if len(dirs) == 0 {
		return []string{"/mods", "/plugins"}, nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		normalized, err := normalizeCurseForgeDirectory(dir)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out, nil
}

func normalizeCurseForgeFilePath(p string, allowed map[string]struct{}) (string, error) {
	cleaned := path.Clean("/" + strings.TrimSpace(p))
	if cleaned == "." || cleaned == "/" {
		return "", fmt.Errorf("file_path is required")
	}
	for dir := range allowed {
		if cleaned == dir || strings.HasPrefix(cleaned, dir+"/") {
			return cleaned, nil
		}
	}
	return "", fmt.Errorf("file_path must be within allowed directories")
}

func normalizeCurseForgeFileName(name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", fmt.Errorf("file_name is required")
	}
	if strings.Contains(trimmed, "/") || strings.Contains(trimmed, "\\") {
		return "", fmt.Errorf("file_name must not include path separators")
	}
	if trimmed == "." || trimmed == ".." {
		return "", fmt.Errorf("file_name must be a file")
	}
	return trimmed, nil
}

func parseCurseForgeSHA1(raw string) ([]byte, string, error) {
	cleaned := strings.TrimSpace(raw)
	if cleaned == "" {
		return nil, "", fmt.Errorf("file_hash is required")
	}
	if len(cleaned) >= len("sha1:") && strings.EqualFold(cleaned[:len("sha1:")], "sha1:") {
		cleaned = cleaned[len("sha1:"):]
	}
	if cleaned == "" {
		return nil, "", fmt.Errorf("file_hash is required")
	}

	lower := strings.ToLower(cleaned)
	if len(lower) == sha1.Size*2 && isHex(lower) {
		b, err := hex.DecodeString(lower)
		if err != nil {
			return nil, "", err
		}
		return b, lower, nil
	}

	return nil, "", fmt.Errorf("file_hash must be a sha1 hex string")
}

func resolveCurseForgeMaxFileSize(requestMax int64) int64 {
	if requestMax > 0 {
		return requestMax
	}
	return curseforgeMaxFileSizeBytes
}

func parseCurseForgeDownloadSpec(data map[string]interface{}, requestMax int64, forceOverwrite bool) (curseforgeDownloadSpec, error) {
	modIDRaw, err := optionalInt64Field(data, "mod_id")
	if err != nil {
		return curseforgeDownloadSpec{}, err
	}
	modID := int(modIDRaw)

	fileIDRaw, err := optionalInt64Field(data, "file_id")
	if err != nil {
		return curseforgeDownloadSpec{}, err
	}
	fileID := int(fileIDRaw)

	fileURLRaw, err := requireStringField(data, "file_url")
	if err != nil {
		return curseforgeDownloadSpec{}, err
	}
	fileNameRaw, err := requireStringField(data, "file_name")
	if err != nil {
		return curseforgeDownloadSpec{}, err
	}
	fileHashRaw, err := requireStringField(data, "file_hash")
	if err != nil {
		return curseforgeDownloadSpec{}, err
	}
	directoryRaw, err := requireStringField(data, "directory")
	if err != nil {
		return curseforgeDownloadSpec{}, err
	}

	fileURL, err := parseCurseForgeURL(fileURLRaw)
	if err != nil {
		return curseforgeDownloadSpec{}, err
	}

	fileName, err := normalizeCurseForgeFileName(fileNameRaw)
	if err != nil {
		return curseforgeDownloadSpec{}, err
	}

	directory, err := normalizeCurseForgeDirectory(directoryRaw)
	if err != nil {
		return curseforgeDownloadSpec{}, err
	}

	hashBytes, hashHex, err := parseCurseForgeSHA1(fileHashRaw)
	if err != nil {
		return curseforgeDownloadSpec{}, err
	}

	overwrite, _ := optionalBoolField(data, "overwrite")
	if forceOverwrite {
		overwrite = true
	}

	isBlocked, _ := optionalBoolField(data, "is_blocked")

	itemMax, err := optionalInt64Field(data, "max_file_size")
	if err != nil {
		return curseforgeDownloadSpec{}, err
	}
	if itemMax <= 0 {
		itemMax = requestMax
	}

	return curseforgeDownloadSpec{
		modID:        modID,
		fileID:       fileID,
		fileURL:      fileURL,
		fileName:     fileName,
		fileHashSHA1: hashBytes,
		fileHashHex:  hashHex,
		directory:    directory,
		overwrite:    overwrite,
		maxFileSize:  resolveCurseForgeMaxFileSize(itemMax),
		isBlocked:    isBlocked,
	}, nil
}

func deleteCurseForgeFileIfExists(s *server.Server, filePath string) (bool, error) {
	if err := s.Filesystem().IsIgnored(filePath); err != nil {
		return false, err
	}
	st, err := s.Filesystem().Stat(filePath)
	if err != nil {
		if errors.Is(err, ufs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if st.IsDir() {
		return false, errors.New("target path is a directory")
	}
	if err := s.Filesystem().Delete(filePath); err != nil {
		return false, err
	}
	return true, nil
}

func newCurseForgeHTTPClient() *http.Client {
	dialer := &net.Dialer{}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		ipStr, _, err := net.SplitHostPort(c.RemoteAddr().String())
		if err != nil {
			return c, err
		}
		ip := net.ParseIP(ipStr)
		if ip == nil {
			return c, errors.New("invalid IP address")
		}
		if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() {
			return c, errors.New("internal IP resolution blocked")
		}
		for _, block := range curseforgeBlockedRanges {
			if block.Contains(ip) {
				return c, errors.New("internal IP resolution blocked")
			}
		}
		return c, nil
	}

	return &http.Client{
		Timeout:   12 * time.Hour,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

