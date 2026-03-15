import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";

type FreshnessType = "current" | "latest_available" | "lagged" | "refresh_failed";

interface FreshnessBadgeProps {
  status: string;
  asOf?: string;
  className?: string;
  lagDays?: number;
  dotOnly?: boolean;
}

export function FreshnessBadge({ status, asOf, className, lagDays, dotOnly }: FreshnessBadgeProps) {
  let colorClass = "";
  let dotClass = "";
  let displayStatus = status.toUpperCase().replace("_", " ");

  if (status === "lagged" && lagDays) {
    displayStatus = `LAGGED (${lagDays}d lag)`;
  }

  switch (status.toLowerCase() as FreshnessType) {
    case "current":
      colorClass = "text-success";
      dotClass = "bg-success";
      break;
    case "latest_available":
      colorClass = "text-primary";
      dotClass = "bg-primary";
      displayStatus = "LATEST";
      break;
    case "lagged":
      colorClass = "text-warning";
      dotClass = "bg-warning";
      break;
    case "refresh_failed":
      colorClass = "text-destructive";
      dotClass = "bg-destructive";
      displayStatus = "CACHED";
      break;
    default:
      colorClass = "text-muted-foreground";
      dotClass = "bg-muted-foreground";
  }

  const badge = dotOnly ? (
    <div className={cn("inline-flex items-center", className)}>
      <div className={cn("h-1.5 w-1.5 rounded-full", dotClass)} />
    </div>
  ) : (
    <div className={cn("inline-flex items-center space-x-1", className)}>
      <div className={cn("h-1.5 w-1.5 rounded-full flex-shrink-0", dotClass)} />
      <span className={cn("text-[9px] font-mono font-semibold tracking-widest", colorClass)}>
        {displayStatus}
      </span>
    </div>
  );

  if (!asOf) return badge;

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div className="cursor-help">{badge}</div>
      </TooltipTrigger>
      <TooltipContent side="top" className="font-mono text-xs">
        As of: {asOf}
      </TooltipContent>
    </Tooltip>
  );
}
