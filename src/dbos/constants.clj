(ns dbos.constants
  "Constants for DBOS workflow statuses and related status sets.
  See: dev.dbos.transact.workflow.WorkflowState")

;; Workflow Status Constants
;; --------------------------

(def status-pending
  "Workflow has been started and is currently executing."
  "PENDING")

(def status-enqueued
  "Workflow has been queued but not yet started execution."
  "ENQUEUED")

(def status-success
  "Workflow completed successfully."
  "SUCCESS")

(def status-error
  "Workflow encountered an error during execution."
  "ERROR")

(def status-cancelled
  "Workflow was cancelled before completion."
  "CANCELLED")

(def status-max-recovery-attempts-exceeded
  "Workflow exceeded the maximum number of recovery attempts after errors."
  "MAX_RECOVERY_ATTEMPTS_EXCEEDED")

;; Status Sets
;; -----------

(def in-progress-statuses
  "Set of statuses indicating workflow is still running or queued."
  #{status-pending status-enqueued})

(def error-statuses
  "Set of statuses indicating workflow failed or was terminated."
  #{status-error status-cancelled status-max-recovery-attempts-exceeded})

(def terminal-statuses
  "Set of statuses indicating workflow has finished (successfully or not)."
  #{status-success status-error status-cancelled status-max-recovery-attempts-exceeded})

(def all-statuses
  "Set of all possible DBOS workflow statuses."
  #{status-pending
    status-enqueued
    status-success
    status-error
    status-cancelled
    status-max-recovery-attempts-exceeded})
