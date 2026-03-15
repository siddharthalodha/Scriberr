package queue

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"scriberr/internal/config"
	"scriberr/internal/models"
	"scriberr/internal/repository"
	"scriberr/pkg/logger"

	"github.com/google/uuid"
)

// StartUploadDirWatcher runs a background goroutine that periodically scans
// the upload directory for new audio files and enqueues them for transcription.
// It only runs when at least one user has auto_watch_upload_dir_enabled = true.
func StartUploadDirWatcher(
	parentCtx context.Context,
	cfg *config.Config,
	userRepo repository.UserRepository,
	profileRepo repository.ProfileRepository,
	jobRepo repository.JobRepository,
	taskQueue *TaskQueue,
) {
	go func() {
		logger.Info("Upload dir watcher goroutine started", "upload_dir", cfg.UploadDir)
		for {
			select {
			case <-parentCtx.Done():
				logger.Info("Upload dir watcher stopped")
				return
			default:
			}

			user, interval, ok := loadWatcherSettings(parentCtx, userRepo)
			if !ok {
				select {
				case <-time.After(30 * time.Second):
					continue
				case <-parentCtx.Done():
					return
				}
			}

			scanUploadDir(parentCtx, cfg.UploadDir, jobRepo, profileRepo, user, taskQueue)

			select {
			case <-time.After(interval):
				continue
			case <-parentCtx.Done():
				return
			}
		}
	}()
}

func loadWatcherSettings(
	ctx context.Context,
	userRepo repository.UserRepository,
) (*models.User, time.Duration, bool) {
	user, err := userRepo.FindFirstWithAutoWatch(ctx)
	if err != nil || user == nil {
		return nil, 0, false
	}
	interval := user.AutoWatchIntervalSeconds
	if interval <= 0 {
		interval = 30
	}
	if interval < 5 {
		interval = 5
	}
	if interval > 3600 {
		interval = 3600
	}
	return user, time.Duration(interval) * time.Second, true
}

func isWatchableAudioFile(ext string) bool {
	switch strings.ToLower(ext) {
	case ".mp3", ".wav", ".m4a", ".flac", ".ogg", ".aac", ".wma":
		return true
	default:
		return false
	}
}

func scanUploadDir(
	ctx context.Context,
	uploadDir string,
	jobRepo repository.JobRepository,
	profileRepo repository.ProfileRepository,
	user *models.User,
	taskQueue *TaskQueue,
) {
	if uploadDir == "" {
		return
	}

	_ = filepath.WalkDir(uploadDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if !isWatchableAudioFile(filepath.Ext(path)) {
			return nil
		}

		exists, err := jobRepo.ExistsByAudioPath(ctx, path)
		if err != nil || exists {
			return nil
		}

		jobID := uuid.New().String()
		title := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))

		job := models.TranscriptionJob{
			ID:        jobID,
			AudioPath: path,
			Title:     &title,
			Status:    models.StatusUploaded,
		}

		if profile := selectWatcherProfile(ctx, profileRepo, user); profile != nil {
			job.Parameters = profile.Parameters
			job.Diarization = profile.Parameters.Diarize
			job.Status = models.StatusPending
		}

		if err := jobRepo.Create(ctx, &job); err != nil {
			logger.Error("Auto-watch: failed to create job", "audio_path", path, "error", err)
			return nil
		}

		logger.Info("Auto-watch: discovered new audio file", "job_id", jobID, "audio_path", path)

		if job.Status == models.StatusPending {
			if err := taskQueue.EnqueueJob(jobID); err != nil {
				logger.Error("Auto-watch: failed to enqueue job", "job_id", jobID, "error", err)
			} else {
				logger.Info("Auto-watch: enqueued job for transcription", "job_id", jobID)
			}
		}

		return nil
	})
}

func selectWatcherProfile(
	ctx context.Context,
	profileRepo repository.ProfileRepository,
	user *models.User,
) *models.TranscriptionProfile {
	if user != nil && user.DefaultProfileID != nil {
		if p, err := profileRepo.FindByID(ctx, *user.DefaultProfileID); err == nil && p != nil {
			return p
		}
	}
	if p, err := profileRepo.FindDefault(ctx); err == nil && p != nil {
		return p
	}
	profiles, _, err := profileRepo.List(ctx, 0, 1)
	if err != nil || len(profiles) == 0 {
		return nil
	}
	return &profiles[0]
}
