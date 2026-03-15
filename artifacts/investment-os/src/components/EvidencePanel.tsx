import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { FreshnessBadge } from "./FreshnessBadge";
import type { EvidenceItem } from "@workspace/api-client-react/src/generated/api.schemas";

export function EvidencePanel({ evidence }: { evidence: EvidenceItem[] }) {
  if (!evidence || evidence.length === 0) return null;

  return (
    <Accordion type="single" collapsible className="w-full mt-4 border-t border-border/50">
      <AccordionItem value="evidence" className="border-none">
        <AccordionTrigger className="py-3 text-xs font-mono font-medium tracking-wider text-muted-foreground hover:text-foreground uppercase hover:no-underline">
          View Supporting Evidence ({evidence.length})
        </AccordionTrigger>
        <AccordionContent className="space-y-3 pb-4">
          {evidence.map((item, idx) => (
            <div key={idx} className="flex flex-col space-y-1 p-3 rounded-md bg-secondary/30 border border-border/50 text-sm">
              <div className="flex items-center justify-between mb-1">
                <div className="flex items-center space-x-2">
                  <span className="font-mono text-xs text-muted-foreground">{item.date}</span>
                  <span className="text-xs font-medium text-foreground bg-secondary px-1.5 py-0.5 rounded">{item.source}</span>
                </div>
                <FreshnessBadge status={item.freshness} />
              </div>
              <p className="text-muted-foreground leading-relaxed">{item.fact}</p>
            </div>
          ))}
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  );
}
