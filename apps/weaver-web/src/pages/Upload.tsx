import { useNavigate } from "react-router";
import { PageHeader } from "@/components/PageHeader";
import { UploadNzbForm } from "@/features/upload/components/UploadNzbForm";
import { useTranslate } from "@/lib/context/translate-context";

export function Upload() {
  const navigate = useNavigate();
  const t = useTranslate();

  return (
    <div className="space-y-6">
      <PageHeader
        title={t("upload.title")}
        description={t("upload.accepts")}
      />
      <UploadNzbForm onSubmitted={() => navigate("/")} />
    </div>
  );
}
