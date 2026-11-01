# GH Archive event schemas

This note records what the raw GH Archive files actually contain. It backs the metric definitions in the [README](../README.md#metric-definitions). Every number below comes from files downloaded on November 1, 2026.

## Files inspected

| File | Purpose |
| --- | --- |
| `2026-10-31-15.json.gz` | One recent hour, for the current schema |
| `2026-10-30-{0..23}.json.gz` | One recent day, for coverage and duplicates |
| Hour 15 of one Wednesday per quarter, 2024 to 2026 | When the schema and the volume changed |
| `2025-06-18-{0..23}.json.gz` | One day before the change, for the candidate repositories |

## Finding 1: pull-request payloads are trimmed

GitHub removed most fields from the public Events API in the fall of 2025. GitHub cited scraping abuse as the reason. The change shows up in the archive between August 27 and October 29, 2025.

| Field a metric needs | Before (Aug. 2025) | After (Oct. 2025 onward) |
| --- | --- | --- |
| `PullRequestEvent.payload.pull_request.created_at` | present | gone |
| `PullRequestEvent.payload.pull_request.merged_at` | present | gone |
| `PullRequestEvent.payload.pull_request.merged` | present | gone; a new action `merged` replaces it |
| `PullRequestEvent.payload.pull_request.title` | present | gone |
| `PullRequestEvent.payload.pull_request.labels` | present | only on `labeled` events |
| `PullRequestEvent.payload.pull_request.head.ref` | present | present |
| `PushEvent.payload.commits` (messages) | present | gone |
| `IssuesEvent.payload.issue` (`created_at`, `closed_at`, `labels`, `state_reason`) | present | present |
| `ReleaseEvent.payload.release` (`published_at`, `draft`, `prerelease`) | present | present |

After the change, lead time needs two events of the same pull request: `opened` and `merged`. Change failure rate loses PR titles and commit messages and keeps only branch names.

## Finding 2: recent coverage is a sparse sample

The archive records far fewer events than it used to, and the mix swings from week to week.

| Hour (15:00 UTC) | Events | Pull-request events | Push events |
| --- | ---: | ---: | ---: |
| 2024-10-30 | 264,593 | 17,784 | 146,372 |
| 2025-04-30 | 252,781 | 17,666 | 155,052 |
| 2025-10-29 | 148,430 | 9,033 | 101,859 |
| 2026-07-29 | 160,280 | 203 | 151,608 |
| 2026-10-07 | 6,218 | 2,270 | 0 |
| 2026-10-28 | 3,004 | 1,043 | 0 |

On October 30, 2026, the whole day held 98,707 events. `kubernetes/kubernetes` appeared 26 times, with two merged pull requests. Of 13,935 merged pull requests that day, only 144 also had their `opened` event in the same day.

## Finding 3: quirks the pipeline must handle

- Duplicates. The October 30 day held 11 event ids twice. The Spark job deduplicates on `id`.
- Missing repository names. Seven `ForkEvent` records had an empty `repo` object. The job drops events without `repo.name`.
- No trailing newline. Some hourly files end without one, so concatenating them with `zcat` glues two records together. Spark reads each file separately and is not affected.
- Drafts and pre-releases. About 16 percent of published releases are pre-releases, and a few are drafts.

## Conclusion

Data from October 2025 onward cannot support lead time or change failure rate for specific repositories. The fields are gone, and the sample is too thin to pair events. Dorado therefore runs on a fixed window before the change. See the README for the window and the definitions.
