(ns make
  "Release automation.

  `bb tag 0.3.0` promotes the changelog's Unreleased section into a dated
  release, bumps the README coordinates, then commits, tags and pushes.
  Publishing to Clojars is a separate step (`bb release`), so a botched tag
  never becomes a botched deploy."
  (:require
   [babashka.process :as process]
   [babashka.tasks :refer [shell]]
   [clojure.string :as str]))

(def ^:private repo-url "https://github.com/shipclojure/dbos-clj")
(def ^:private changelog "CHANGELOG.md")
(def ^:private readme "README.md")

;; -- helpers -----------------------------------------------------------------

(defn- fail! [& msg]
  (throw (ex-info (str/join " " msg) {})))

(defn- replace-in-file!
  "Apply `match` -> `replacement` to `file`. Fails when nothing matched: a
  silent no-op would leave a half-prepared release."
  [file match replacement]
  (let [before (slurp file)
        after (str/replace before match replacement)]
    (when (= before after)
      (fail! "Nothing matched in" file "for" (pr-str match)))
    (spit file after)))

(defn- git [& args]
  (let [{:keys [exit out err]} (process/sh (into ["git"] args))]
    (when-not (zero? exit)
      (fail! "git" (str/join " " args) "failed:" (str/trim err)))
    (str/trim out)))

(defn- version-tags
  "All `v*` tags, newest first, without the leading v.

  Sorted by version rather than `git describe`, which resolves by commit
  topology — that picks arbitrarily when two tags share a commit, and would
  silently produce the wrong compare link."
  []
  (let [{:keys [exit out]} (process/sh ["git" "tag" "-l" "v*" "--sort=-v:refname"])]
    (when (zero? exit)
      (->> (str/split-lines (str/trim out))
           (remove str/blank?)
           (mapv #(str/replace % #"^v" ""))))))

(defn- latest-version
  "Highest `v*` tag, prereleases included."
  []
  (first (version-tags)))

(defn- previous-release
  "Highest stable `v*` tag, for the compare link.

  Prereleases are skipped: they get squashed into the release's single
  changelog entry, so linking to v0.4.0-alpha2 would show a diff that
  doesn't match what that entry describes."
  []
  (first (filter #(re-matches #"\d+\.\d+\.\d+" %) (version-tags))))

(defn- changelog-sections
  "Parse the changelog into {version body}. Link-reference lines are dropped so
  the oldest section doesn't swallow them."
  []
  (->> (str/split (slurp changelog) #"(?m)^## ")
       rest
       (map (fn [chunk]
              (let [[heading & body] (str/split-lines chunk)]
                [(second (re-find #"^\[([^\]]+)\]" heading))
                 (->> body
                      (remove #(re-matches #"\[[^\]]+\]: .*" %))
                      (str/join "\n")
                      str/trim)])))
       (into {})))

;; -- secrets -----------------------------------------------------------------

(defn op-read
  "Read a secret from 1Password. Throws rather than returning a blank, which
  would otherwise surface as a confusing 401 from Clojars."
  [reference]
  (let [{:keys [exit out err]} (process/sh ["op" "read" reference])
        secret (str/trim out)]
    (cond
      (not (zero? exit))
      (fail! "1Password lookup failed for" reference "-" (str/trim err)
             "\n  Is `op` installed and signed in?")

      (str/blank? secret)
      (fail! "1Password returned an empty value for" reference)

      :else secret)))

;; -- release steps -----------------------------------------------------------

(defn- promote-unreleased!
  "Rename `## [Unreleased]` to `## [version] - <today>`, leaving a fresh empty
  Unreleased section above it."
  [version]
  (replace-in-file! changelog
                    #"(?m)^## \[Unreleased\]$"
                    (format "## [Unreleased]\n\n## [%s] - %s"
                            version (str (java.time.LocalDate/now)))))

(defn- relink-changelog!
  "Point [unreleased] at the new tag and add the release's compare link."
  [version previous]
  (replace-in-file! changelog
                    #"(?m)^\[unreleased\]: .*$"
                    (format "[unreleased]: %s/compare/v%s...HEAD\n[%s]: %s/compare/v%s...v%s"
                            repo-url version version repo-url previous version)))

(defn- bump-readme!
  "Update the deps and lein coordinates in the install snippets."
  [version]
  (doseq [re [#"(com\.shipclojure/dbos-clj \{:mvn/version \")[^\"]*(\")"
              #"(\[com\.shipclojure/dbos-clj \")[^\"]*(\")"]]
    ;; a fn replacement, not "$1...$2" — `$1` followed by a digit would be read
    ;; as group 1x
    (replace-in-file! readme re (fn [[_ before after]] (str before version after)))))

(defn- ensure-releasable! [version]
  ;; MAJOR.MINOR.PATCH with an optional semver prerelease suffix, e.g.
  ;; 0.4.0-alpha1 — dot-separated alphanumeric identifiers.
  (when-not (re-matches #"\d+\.\d+\.\d+(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?" version)
    (fail! "Version must be MAJOR.MINOR.PATCH[-PRERELEASE], got:" version))
  (when (seq (git "status" "--porcelain"))
    (fail! "Working tree is dirty — commit or stash first."))
  (when (seq (git "tag" "-l" (str "v" version)))
    (fail! (str "Tag v" version " already exists.")))
  (let [sections (changelog-sections)]
    (when-not (contains? sections "Unreleased")
      (fail! "No `## [Unreleased]` section in" changelog))
    (when (str/blank? (get sections "Unreleased"))
      (fail! "The [Unreleased] section is empty — nothing to release."))))

;; -- tasks -------------------------------------------------------------------

(defn tag
  "Prepare and publish a release. Usage: bb tag 0.3.0"
  [& [version]]
  (let [version (some-> version str/trim not-empty)]
    (when-not version
      (fail! "Usage: bb tag <version>   e.g. bb tag 0.3.0"))
    (ensure-releasable! version)
    (let [previous (or (previous-release)
                       (fail! "No previous stable v* tag to compare the release against."))]
      (shell "git fetch origin")
      (shell "git pull origin HEAD")
      (promote-unreleased! version)
      (relink-changelog! version previous)
      (bump-readme! version)
      (shell "git add" changelog readme)
      (shell "git commit -m" (str "Release: " version))
      (shell "git tag" (str "v" version))
      (shell "git push origin HEAD")
      (shell "git push origin --tags")
      (println (format "\nTagged v%s (was v%s). Publish it with: bb release"
                       version previous)))))

(defn changelog-entry
  "Print one release's changelog section, for GitHub release notes.
  Defaults to the latest tag. Usage: bb changelog-entry [0.2.0]"
  [& [version]]
  (let [version (or (some-> version str/trim not-empty) (latest-version))
        sections (changelog-sections)]
    (if-let [body (get sections version)]
      (println body)
      (fail! "No changelog section for" version "— have:"
             (str/join ", " (sort (keys sections)))))))
