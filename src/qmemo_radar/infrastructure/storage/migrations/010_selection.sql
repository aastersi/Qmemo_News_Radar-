BEGIN;

-- One story told by one or more stored texts. The representative is the first text of the story;
-- only it is ranked. Aggregates are refreshed from its texts and their copies whenever it grows.
CREATE TABLE event_clusters (
    id INTEGER PRIMARY KEY,
    representative_event_id TEXT NOT NULL UNIQUE,
    language TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    member_count INTEGER NOT NULL DEFAULT 1,
    mention_count INTEGER NOT NULL DEFAULT 1,
    domain_count INTEGER NOT NULL DEFAULT 1,
    -- Distinct articles (titles): syndicated reprints of one article count once.
    article_count INTEGER NOT NULL DEFAULT 1,
    source_count INTEGER NOT NULL DEFAULT 1,
    -- Article of the representative: one preselected quote per article.
    article TEXT,
    -- candidate | preselected | sibling (another quote of a preselected article) | boilerplate
    state TEXT NOT NULL DEFAULT 'candidate',
    -- -1 until scored: a run that failed after clustering leaves these for the next run.
    preselect_score INTEGER NOT NULL DEFAULT -1,
    -- Breakdown and notes, kept only for preselected and boilerplate stories.
    preselect_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(representative_event_id) REFERENCES radar_events(id) ON DELETE CASCADE
);
CREATE INDEX idx_clusters_state_score ON event_clusters(state, preselect_score DESC);
CREATE INDEX idx_clusters_last_seen ON event_clusters(last_seen_at);
CREATE INDEX idx_clusters_article ON event_clusters(article) WHERE state = 'preselected';

ALTER TABLE radar_events ADD COLUMN cluster_id INTEGER
    REFERENCES event_clusters(id) ON DELETE SET NULL;
-- Host of the URL without www.: distinct domains of a story are counted from it.
ALTER TABLE radar_events ADD COLUMN domain TEXT;
-- Short hash of the article title (or URL): distinct articles of a story are counted from it.
ALTER TABLE radar_events ADD COLUMN article TEXT;
CREATE INDEX idx_events_cluster ON radar_events(cluster_id) WHERE cluster_id IS NOT NULL;
-- Texts waiting for clustering: DISCOVERED and not yet in a cluster.
CREATE INDEX idx_events_unclustered ON radar_events(discovered_at)
    WHERE cluster_id IS NULL AND status = 'DISCOVERED';

-- MinHash-LSH band keys of cluster representatives (members are not indexed, so a story can
-- never drift through a chain of ever less similar texts). A derived index: no foreign key,
-- `qmemo-radar prune` removes the keys of deleted stories and of stories past the window.
CREATE TABLE cluster_keys (
    band_key INTEGER NOT NULL,
    cluster_id INTEGER NOT NULL,
    PRIMARY KEY (band_key, cluster_id)
) WITHOUT ROWID;

-- Exact copies of a stored text elsewhere: one light row each instead of a second event row
-- with the whole payload. The text's own URL is in radar_events.
CREATE TABLE content_mentions (
    event_id TEXT NOT NULL REFERENCES radar_events(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    domain TEXT NOT NULL,
    article TEXT,
    source_key TEXT,
    seen_at TEXT NOT NULL,
    PRIMARY KEY (event_id, url)
) WITHOUT ROWID;

-- A bounded sample of what the gate rejected, for `qmemo-radar rejected`; counts are in
-- pipeline_metrics (rejected_<reason>).
CREATE TABLE rejected_samples (
    id INTEGER PRIMARY KEY,
    reason TEXT NOT NULL,
    source_key TEXT,
    text TEXT NOT NULL,
    url TEXT,
    seen_at TEXT NOT NULL
);
CREATE INDEX idx_rejected_reason ON rejected_samples(reason, id);

INSERT INTO schema_migrations(version, applied_at)
VALUES (10, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

COMMIT;
