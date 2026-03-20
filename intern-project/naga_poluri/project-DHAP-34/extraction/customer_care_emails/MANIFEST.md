# Dataset Manifest — customer_care_emails

**Project:** DHAP-34
**Author:** naga_poluri
**Repo:** https://github.com/The-Nagaa/airflow-dag-configs

---

## Dataset Info

| Field | Value |
|-------|-------|
| Dataset Name | customer_care_emails |
| Dataset Code | 102 |
| Source | https://huggingface.co/datasets/rtweera/customer_care_emails |
| Analysis Status | Completed |
| Relevance | Customer Frustration Analysis |

## File Paths

| Item | Path |
|------|------|
| Local CSV Folder | `extraction/customer_care_emails/sample_data/` |
| Target Table | `public.customer_care_emails` |
| Schema Contract | `extraction/customer_care_emails/config/schema_expected.yaml` |
| DDL File | `extraction/customer_care_emails/config/create_table.sql` |
| DAG File | `extraction/customer_care_emails/dags/customer_care_emails_ingest.py` |

## About the Dataset

This dataset has customer support email threads with sentiment scores,
issue categories, and resolution status. Each row represents one email
interaction with a customer.

Important: rows where `status = 'done'` were already processed in a
previous pipeline run, so the DAG skips them automatically.

## Notes

- CSV is downloaded from SharePoint and placed manually in `sample_data/`
- Never commit real credentials — only `.env.sample` goes to Git
- Update `schema_expected.yaml` and `create_table.sql` if columns change
