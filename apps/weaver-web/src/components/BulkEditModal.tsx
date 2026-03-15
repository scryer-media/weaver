import { useEffect, useState } from "react";
import { useQuery } from "urql";
import { CATEGORIES_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const NO_CHANGE = "__no_change__";

interface BulkEditModalProps {
  open: boolean;
  selectedCount: number;
  onClose: () => void;
  onApply: (category: string | null, priority: string | null) => void;
}

export function BulkEditModal({ open, selectedCount, onClose, onApply }: BulkEditModalProps) {
  const t = useTranslate();
  const [category, setCategory] = useState(NO_CHANGE);
  const [priority, setPriority] = useState(NO_CHANGE);
  const [{ data: categoryData }] = useQuery({ query: CATEGORIES_QUERY });
  const categories = categoryData?.categories ?? [];

  useEffect(() => {
    if (open) {
      setCategory(NO_CHANGE);
      setPriority(NO_CHANGE);
    }
  }, [open]);

  const handleApply = () => {
    onApply(
      category === NO_CHANGE ? null : category,
      priority === NO_CHANGE ? null : priority,
    );
  };

  return (
    <Dialog open={open} onOpenChange={(next) => (!next ? onClose() : undefined)}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>{t("bulk.editTitle")}</DialogTitle>
          <DialogDescription>
            {t("bulk.selected", { count: selectedCount })}
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-4">
          <div className="space-y-2">
            <Label>{t("table.category")}</Label>
            <Select value={category} onValueChange={setCategory}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={NO_CHANGE}>{t("bulk.noChange")}</SelectItem>
                {(categories as { id: number; name: string }[]).map((entry) => (
                  <SelectItem key={entry.id} value={entry.name}>
                    {entry.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>{t("upload.priorityLabel")}</Label>
            <Select value={priority} onValueChange={setPriority}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={NO_CHANGE}>{t("bulk.noChange")}</SelectItem>
                <SelectItem value="LOW">{t("upload.priorityLow")}</SelectItem>
                <SelectItem value="NORMAL">{t("upload.priorityNormal")}</SelectItem>
                <SelectItem value="HIGH">{t("upload.priorityHigh")}</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
        <DialogFooter>
          <Button variant="ghost" onClick={onClose}>
            {t("action.cancel")}
          </Button>
          <Button onClick={handleApply}>
            {t("action.apply")}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
