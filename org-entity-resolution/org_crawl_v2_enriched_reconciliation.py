import os
from datetime import date
from collections import deque

import pandas as pd
import requests


BASE_URL = "https://api.example.com/v1"
EXPORT_FOLDER = "./exports"
STARTING_USER_IDS = ["user-ROOT-ID-PLACEHOLDER"]

# For local testing only:
# Set your token in your environment before running the script.
# Example (Mac/Linux):
# export API_TOKEN="your_actual_token_here"


def get_api_token() -> str:
    """
    Read the API token from an environment variable.
    """
    token = os.getenv("API_TOKEN")
    if not token:
        raise ValueError(
            "Missing API_TOKEN environment variable."
        )
    return token


def build_session(token: str) -> requests.Session:
    """
    Create a reusable requests session with shared headers.
    """
    session = requests.Session()
    session.headers.update(
        {
            "accept": "application/json",
            "authorization": f"Bearer {token}",
        }
    )
    return session


def get_json(session: requests.Session, url: str):
    """
    Send a GET request and return parsed JSON.
    Returns None if the request fails.
    """
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exc:
        print(f"Request failed for {url}: {exc}")
        return None


def fetch_direct_reports(user_id: str, session: requests.Session) -> list[dict]:
    """
    Phase 1:
    Fetch direct reports for one user from /user/{user_id}/directReports
    """
    url = f"{BASE_URL}/user/{user_id}/directReports"
    data = get_json(session, url)
    if not data:
        return []
    return data.get("data", [])


def fetch_full_user_record(user_id: str, session: requests.Session) -> dict | None:
    """
    Phase 2:
    Retrieve the complete user record from /user/{user_id}
    """
    url = f"{BASE_URL}/user/{user_id}"
    return get_json(session, url)


def normalize_org_data(user_list: list[dict]) -> pd.DataFrame:
    """
    Flatten org crawl results into a DataFrame.
    """
    if not user_list:
        return pd.DataFrame()

    df = pd.json_normalize(user_list, sep="_").rename(
        columns={
            "manager_id": "ManagerID",
            "department_id": "DepartmentID",
        }
    )

    return df


def crawl_org_with_levels(starting_ids: list[str], session: requests.Session) -> pd.DataFrame:
    """
    Phase 1:
    Discover everyone in the org tree by traversing the direct reports endpoint.
    Also track the hierarchy level for each person.
    """
    seen_ids = set()
    queue = deque((user_id, 0) for user_id in starting_ids)
    all_users = []
    user_name_lookup = {}

    # Add root user(s) first so they appear in the final dataset as Level 0
    for root_id in starting_ids:
        root_user = fetch_full_user_record(root_id, session)
        if root_user:
            root_user["Level"] = 0
            all_users.append(root_user)
            user_name_lookup[root_user["id"]] = root_user.get("name")

    # Traverse the org structure
    while queue:
        current_id, current_level = queue.popleft()

        if current_id in seen_ids:
            continue
        seen_ids.add(current_id)

        direct_reports = fetch_direct_reports(current_id, session)

        for report in direct_reports:
            report_id = report.get("id")
            if not report_id:
                continue

            report["Level"] = current_level + 1
            all_users.append(report)
            user_name_lookup[report_id] = report.get("name")

            if report_id not in seen_ids:
                queue.append((report_id, current_level + 1))

    org_df = normalize_org_data(all_users)

    if not org_df.empty:
        org_df = org_df.drop_duplicates(subset=["id"]).copy()

        if "ManagerID" in org_df.columns:
            org_df["ManagerName"] = org_df["ManagerID"].map(user_name_lookup)

    return org_df


def fetch_all_full_user_records(user_ids: list[str], session: requests.Session) -> pd.DataFrame:
    """
    Phase 2:
    Use the IDs discovered in the org crawl and call /user/{user_id}
    for each person to retrieve full user records.
    """
    detailed_records = []

    for user_id in user_ids:
        user_record = fetch_full_user_record(user_id, session)
        if user_record:
            detailed_records.append(user_record)

    if not detailed_records:
        return pd.DataFrame()

    detail_df = pd.json_normalize(detailed_records, sep="_")
    detail_df = detail_df.drop_duplicates(subset=["id"]).copy()

    return detail_df


def run_qa_checks(df: pd.DataFrame, df_name: str = "DataFrame") -> None:
    """
    Print basic QA checks for row count, duplicate IDs, and null counts.
    """
    print(f"\n=== QA CHECK: {df_name} ===")
    print(f"Rows: {len(df)}")

    if df.empty:
        print("DataFrame is empty.")
        return

    if "id" in df.columns:
        duplicate_ids = int(df["id"].duplicated().sum())
        null_ids = int(df["id"].isna().sum())
        unique_ids = int(df["id"].nunique())
        print(f"Unique IDs: {unique_ids}")
        print(f"Duplicate IDs: {duplicate_ids}")
        print(f"Null IDs: {null_ids}")

    important_columns = ["id", "name", "title", "ManagerID", "Level", "status", "manager_id"]
    available_columns = [col for col in important_columns if col in df.columns]

    if available_columns:
        print("\nNull counts in key columns:")
        for col in available_columns:
            print(f"  - {col}: {int(df[col].isna().sum())}")


def compare_phase1_to_phase2(org_df: pd.DataFrame, detail_df: pd.DataFrame) -> None:
    """
    Compare Phase 1 discovered IDs to Phase 2 detailed user record IDs.
    """
    if org_df.empty or detail_df.empty or "id" not in org_df.columns or "id" not in detail_df.columns:
        print("\nUnable to compare Phase 1 and Phase 2 ID coverage.")
        return

    phase1_ids = set(org_df["id"].dropna().unique())
    phase2_ids = set(detail_df["id"].dropna().unique())

    missing_in_phase2 = sorted(phase1_ids - phase2_ids)

    print("\n=== PHASE 1 vs PHASE 2 RECONCILIATION ===")
    print(f"Phase 1 unique IDs: {len(phase1_ids)}")
    print(f"Phase 2 unique IDs: {len(phase2_ids)}")
    print(f"IDs missing in Phase 2: {len(missing_in_phase2)}")

    if missing_in_phase2:
        print("First few missing IDs:")
        for missing_id in missing_in_phase2[:10]:
            print(f"  - {missing_id}")


def transform_to_microsoft_schema(
    detail_df: pd.DataFrame,
    executive_ids: list[str] | None = None
) -> pd.DataFrame:
    """
    Align full user record output to a Microsoft-ready schema.
    Adds placeholder columns where API data does not yet exist.
    Also derives manager and executive flags.
    """
    executive_ids = executive_ids or []

    column_mapping = {
        "id": "SourceAPIUserID",
        "name": "SourceUserName",
        "preferredName": "SourcePreferredName",
        "email": "SourceEmail",
        "title": "SourceTitle",
        "externalUserId": "SourceExternalUserID",
        "manager_id": "SourceManagerUserID",
        "department_id": "SourceDepartmentID",
        "status": "SourceStatus",
        "startDate": "SourceStartDate",
        "createdAt": "SourceUserCreatedAt",
        "updatedAt": "SourceUserUpdatedAt",
    }

    df = detail_df.copy()
    df = df.rename(columns=column_mapping)

    # Metadata fields
    current_timestamp = pd.Timestamp.utcnow()
    df["APIImportTimeStamp"] = current_timestamp
    df["APIImportLog"] = "User Sync"
    df["APIImportRunBy"] = "Python Script"
    df["LastUpdatedByImport"] = current_timestamp

    # Placeholder / downstream fields
    df["MicrosoftGUID"] = None
    df["UserHistoryLog"] = None
    df["IsBranchDirector"] = None

    # Executive flag from root starting IDs
    df["IsExecutive"] = df["SourceAPIUserID"].isin(executive_ids)

    # Manager flag:
    # anyone whose user ID appears in the manager column of the full user list
    manager_ids = set(df["SourceManagerUserID"].dropna().unique())
    df["IsManager"] = df["SourceAPIUserID"].isin(manager_ids)

    final_columns = [
        "APIImportTimeStamp",
        "APIImportLog",
        "SourceAPIUserID",
        "SourceUserName",
        "SourcePreferredName",
        "SourceEmail",
        "SourceTitle",
        "SourceExternalUserID",
        "SourceManagerUserID",
        "SourceDepartmentID",
        "IsExecutive",
        "IsBranchDirector",
        "IsManager",
        "SourceStatus",
        "SourceStartDate",
        "LastUpdatedByImport",
        "SourceUserCreatedAt",
        "SourceUserUpdatedAt",
        "APIImportRunBy",
        "MicrosoftGUID",
        "UserHistoryLog",
    ]

    for col in final_columns:
        if col not in df.columns:
            df[col] = None

    df = df[final_columns]

    return df


def export_dataframe(df: pd.DataFrame, folder: str, filename_prefix: str) -> str:
    """
    Save a DataFrame to CSV with today's date in the filename.
    """
    os.makedirs(folder, exist_ok=True)
    today_str = date.today().isoformat()
    filepath = os.path.join(folder, f"{filename_prefix}_{today_str}.csv")
    df.to_csv(filepath, index=False)
    return filepath


def main():
    token = get_api_token()
    session = build_session(token)

    # Phase 1: Discover org structure from direct reports
    org_df = crawl_org_with_levels(STARTING_USER_IDS, session)

    if org_df.empty:
        print("No org data returned.")
        return

    print("\n=== ORG PREVIEW ===")
    org_preview_cols = [c for c in ["id", "name", "title", "ManagerName", "Level"] if c in org_df.columns]
    print(org_df[org_preview_cols].head(20))

    run_qa_checks(org_df, "Org Crawl")

    org_file = export_dataframe(org_df, EXPORT_FOLDER, "org_chart_enriched")
    print(f"\nSaved org chart file: {org_file}")

    # Phase 2: Use discovered IDs to pull full user records
    user_ids = org_df["id"].dropna().unique().tolist()
    detail_df = fetch_all_full_user_records(user_ids, session)

    if detail_df.empty:
        print("No detailed user data returned.")
        return

    run_qa_checks(detail_df, "Full User Records")
    compare_phase1_to_phase2(org_df, detail_df)

    print("\n=== FULL USER RECORDS PREVIEW ===")
    detail_preview_cols = [
        c for c in ["id", "name", "title", "email", "status", "manager_id"]
        if c in detail_df.columns
    ]
    print(detail_df[detail_preview_cols].head(5))

    details_file = export_dataframe(detail_df, EXPORT_FOLDER, "full_user_records")
    print(f"Saved full user records file: {details_file}")

    # Phase 3: Transform to Microsoft-ready schema
    microsoft_ready_df = transform_to_microsoft_schema(
        detail_df,
        executive_ids=STARTING_USER_IDS
    )

    print("\n=== MICROSOFT-READY PREVIEW ===")
    ms_preview_cols = [
        c for c in [
            "SourceAPIUserID",
            "SourceUserName",
            "SourceManagerUserID",
            "IsExecutive",
            "IsManager",
            "SourceStatus"
        ]
        if c in microsoft_ready_df.columns
    ]
    print(microsoft_ready_df[ms_preview_cols].head(5))

    run_qa_checks(microsoft_ready_df, "Microsoft Ready User Table")

    ms_file = export_dataframe(
        microsoft_ready_df,
        EXPORT_FOLDER,
        "microsoft_ready_users"
    )
    print(f"Saved Microsoft-ready file: {ms_file}")


if __name__ == "__main__":
    main()
