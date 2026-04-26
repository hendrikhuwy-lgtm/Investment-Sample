import type {
  BlueprintExplorerContract,
  CandidateReportContract,
  ChangesContract,
  CompareContract,
  DailyBriefContract,
  EvidenceWorkspaceContract,
  NotebookContract,
  PortfolioContract,
} from "../../shared/v2_surface_contracts";

const API = "/api/v2";

export class ApiRequestError extends Error {
  userMessage: string;
  developerMessage: string;
  path: string;
  status?: number;

  constructor(userMessage: string, developerMessage: string, path: string, status?: number) {
    super(userMessage);
    this.name = "ApiRequestError";
    this.userMessage = userMessage;
    this.developerMessage = developerMessage;
    this.path = path;
    this.status = status;
  }
}

export type CandidateReportPendingResponse = {
  status: "report_pending" | "report_unavailable";
  surface_id: "candidate_report";
  candidate_id: string;
  generated_at?: string | null;
  source_binding?: {
    source_snapshot_id?: string | null;
    source_generated_at?: string | null;
    source_contract_version?: string | null;
  };
  reason?: string | null;
  message?: string | null;
  retry_after_ms?: number | null;
  report_cache_state?: string | null;
  binding_state?: string | null;
  route_cache_state?: Record<string, unknown> | null;
  report_loading_hint?: Record<string, unknown> | null;
};

export type CandidateReportResponse = CandidateReportContract | CandidateReportPendingResponse;

export type BlueprintCoverageAuditContract = {
  generated_at: string;
  candidate_id?: string | null;
  sleeve_key?: string | null;
  summary: {
    candidate_count: number;
    direct_ready_count: number;
    proxy_ready_count: number;
    suppressed_count: number;
    alias_review_count: number;
  };
  items: Array<{
    candidate_id: string;
    symbol: string;
    name: string;
    sleeve_key: string;
    provider_symbol?: string | null;
    fallback_aliases?: string[];
    direct_bars?: number;
    proxy_bars?: number;
    direct_quality?: { quality_label?: string | null; stale_days?: number | null };
    proxy_quality?: { quality_label?: string | null; stale_days?: number | null };
    coverage_verdict: string;
    support_verdict: string;
    onboarding_checklist: Array<{
      label: string;
      state: string;
      detail?: string | null;
    }>;
  }>;
};

type FetchJsonOptions = {
  method?: "GET" | "POST";
  timeoutMs?: number;
  timeoutMessage?: string;
  failureMessage?: string;
};

async function fetchJson<T>(path: string, options?: FetchJsonOptions): Promise<T> {
  const controller = new AbortController();
  const timeoutMs = options?.timeoutMs ?? 0;
  const timer =
    timeoutMs > 0
      ? window.setTimeout(() => controller.abort(), timeoutMs)
      : null;
  try {
    const response = await fetch(`${API}${path}`, {
      method: options?.method ?? "GET",
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new ApiRequestError(
        options?.failureMessage ?? "Request failed. Please retry from the current view.",
        `HTTP ${response.status} ${path}`,
        path,
        response.status,
      );
    }
    const text = await response.text();
    return JSON.parse(text) as T;
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new ApiRequestError(
        options?.timeoutMessage ?? "Request is taking longer than expected. Cached content will remain visible if available.",
        `Request timed out ${path}`,
        path,
      );
    }
    throw error;
  } finally {
    if (timer !== null) {
      window.clearTimeout(timer);
    }
  }
}

export function fetchHealth() {
  return fetchJson<{ status: string; layer: string }>("/health", {
    timeoutMs: 2_000,
    timeoutMessage: "The V2 API did not respond quickly.",
  });
}

export function fetchBlueprintExplorer(reportCandidateId?: string | null) {
  const timeoutMessage = "Blueprint is taking longer than expected. Cached Blueprint content will remain visible if available.";
  if (!reportCandidateId) {
    return fetchJson<BlueprintExplorerContract>("/surfaces/blueprint/explorer", {
      timeoutMs: 10_000,
      timeoutMessage,
    });
  }
  return fetchJson<BlueprintExplorerContract>(
    `/surfaces/blueprint/explorer?report_candidate_id=${encodeURIComponent(reportCandidateId)}`,
    { timeoutMs: 10_000, timeoutMessage }
  );
}

export function fetchPortfolio(accountId = "default") {
  return fetchJson<PortfolioContract>(
    `/surfaces/portfolio?account_id=${encodeURIComponent(accountId)}`
  );
}

export function fetchDailyBrief(options?: { force?: boolean }) {
  return fetchJson<DailyBriefContract>(`/surfaces/daily-brief${options?.force ? "?force=1" : ""}`, {
    timeoutMs: 8_000,
    timeoutMessage: "Daily Brief is taking longer than expected. Cached Daily Brief content will remain visible if available.",
  });
}

export function fetchCompare(candidateIds: string[], sleeveId?: string | null) {
  const params = new URLSearchParams({
    ids: candidateIds.join(","),
  });
  if (sleeveId) {
    params.set("sleeve_id", sleeveId);
  }
  return fetchJson<CompareContract>(`/blueprint/compare?${params.toString()}`, {
    timeoutMs: 8_000,
    timeoutMessage: "Compare is taking longer than expected. The current Blueprint remains readable.",
  });
}

export type ChangesFetchOptions = {
  sinceUtc?: string | null;
  window?: "today" | "3d" | "7d" | string | null;
  timezone?: string | null;
  category?: string | null;
  sleeveId?: string | null;
  needsReview?: boolean | null;
  limit?: number | null;
  cursor?: string | null;
};

export function fetchChanges(surfaceId: string, options?: string | ChangesFetchOptions | null) {
  const params = new URLSearchParams();
  const opts: ChangesFetchOptions =
    typeof options === "string"
      ? { sinceUtc: options }
      : options ?? {};
  if (opts.sinceUtc) {
    params.set("since_utc", opts.sinceUtc);
  }
  if (opts.window) {
    params.set("window", opts.window);
  }
  if (opts.timezone) {
    params.set("timezone", opts.timezone);
  }
  if (opts.category && opts.category !== "all") {
    params.set("category", opts.category);
  }
  if (opts.sleeveId && opts.sleeveId !== "all") {
    params.set("sleeve_id", opts.sleeveId);
  }
  if (typeof opts.needsReview === "boolean") {
    params.set("needs_review", String(opts.needsReview));
  }
  if (opts.limit) {
    params.set("limit", String(opts.limit));
  }
  if (opts.cursor) {
    params.set("cursor", opts.cursor);
  }
  if (surfaceId === "blueprint_explorer") {
    const query = params.toString();
    return fetchJson<ChangesContract>(
      `/surfaces/blueprint/explorer/changes${query ? `?${query}` : ""}`,
      {
        timeoutMs: 8_000,
        timeoutMessage: "Changes are taking longer than expected. The Blueprint remains readable.",
      }
    );
  }
  params.set("surface_id", surfaceId);
  return fetchJson<ChangesContract>(`/surfaces/changes?${params.toString()}`, {
    timeoutMs: 8_000,
    timeoutMessage: "Changes are taking longer than expected. The current surface remains readable.",
  });
}

export function fetchCandidateReport(
  candidateId: string,
  binding?: {
    sleeveKey?: string | null;
    sourceSnapshotId?: string | null;
    sourceGeneratedAt?: string | null;
    sourceContractVersion?: string | null;
    refresh?: boolean;
  },
) {
  const params = new URLSearchParams();
  if (binding?.sleeveKey) {
    params.set("sleeve_key", binding.sleeveKey);
  }
  if (binding?.sourceSnapshotId) {
    params.set("source_snapshot_id", binding.sourceSnapshotId);
  }
  if (binding?.sourceGeneratedAt) {
    params.set("source_generated_at", binding.sourceGeneratedAt);
  }
  if (binding?.sourceContractVersion) {
    params.set("source_contract_version", binding.sourceContractVersion);
  }
  if (binding?.refresh) {
    params.set("refresh", "true");
  }
  const query = params.toString();
  return fetchJson<CandidateReportResponse>(
    `/surfaces/candidates/${encodeURIComponent(candidateId)}/report-plain${query ? `?${query}` : ""}`,
    {
      timeoutMs: 8_000,
      timeoutMessage: "Report is taking longer than expected. Cached content will remain visible if available.",
    }
  );
}

export function fetchNotebook(candidateId: string) {
  return fetchJson<NotebookContract>(
    `/surfaces/candidates/${encodeURIComponent(candidateId)}/notebook`
  );
}

export function fetchEvidenceWorkspace(candidateId: string) {
  return fetchJson<EvidenceWorkspaceContract>(
    `/surfaces/candidates/${encodeURIComponent(candidateId)}/evidence`
  );
}

export function fetchBlueprintCoverageAudit() {
  return fetchJson<BlueprintCoverageAuditContract>("/admin/blueprint/market-path/coverage-audit");
}

export function requestDeferredForecastStart() {
  return fetchJson<{ status: string; mode?: string; queued_at?: string }>("/admin/forecast/deferred-start", {
    method: "POST",
    timeoutMs: 2_000,
    timeoutMessage: "Forecast startup was deferred until the page finished loading.",
  });
}

export async function uploadPortfolioHoldings(
  csvText: string,
  filename: string
): Promise<{ run_id: string }> {
  const response = await fetch(`${API}/portfolio/uploads`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ csv_text: csvText, filename, source_name: "manual_csv_upload" }),
  });
  if (!response.ok) throw new Error(`Upload failed: HTTP ${response.status}`);
  return response.json() as Promise<{ run_id: string }>;
}

export async function activatePortfolioUpload(runId: string): Promise<void> {
  const response = await fetch(`${API}/portfolio/uploads/${encodeURIComponent(runId)}/activate`, {
    method: "POST",
  });
  if (!response.ok) throw new Error(`Activation failed: HTTP ${response.status}`);
}
