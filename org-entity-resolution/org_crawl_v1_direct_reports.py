import os
from datetime import date

import pandas as pd
import requests


BASE_URL = "https://api.example.com/v1"
STARTING_USER_IDS = ["user-ROOT-ID-PLACEHOLDER"]
EXPORT_FOLDER = "./exports"


# === Get full details of a user by ID
def fetch_user_details(user_id, token):
    url = f"{BASE_URL}/user/{user_id}"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {token}"
    }

    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code != 200:
        print(f"Error fetching user details for {user_id}")
        return None

    return response.json()


# === Fetch direct reports for a given user ID
def fetch_direct_reports(user_id, token):
    url = f"{BASE_URL}/user/{user_id}/directReports"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {token}"
    }

    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code != 200:
        print(f"Error fetching direct reports for {user_id}")
        return []

    return response.json().get("data", [])


# === Normalize a list of user objects into a DataFrame
def normalize_user_data(user_list):
    if not user_list:
        return pd.DataFrame()

    df = pd.json_normalize(
        user_list,
        sep="_",
        meta=[
            "id",
            "name",
            "preferredName",
            "email",
            "title",
            "status",
            "startDate",
            "timezone",
            "externalUserId",
            ["manager", "id"],
            ["department", "id"],
            "Level"
        ]
    ).rename(columns={
        "manager_id": "ManagerID",
        "department_id": "DepartmentID"
    })

    return df


# === Main function to crawl org structure and track Levels
def crawl_org_with_levels(starting_ids, token):
    seen_ids = set()
    queue = [(user_id, 0) for user_id in starting_ids]  # (UserID, Level)
    all_users = []
    manager_lookup = {}

    # Step 1: Add root users (Level 0)
    for root_id in starting_ids:
        root_user = fetch_user_details(root_id, token)
        if root_user:
            root_user["Level"] = 0
            all_users.append(root_user)
            manager_lookup[root_user["id"]] = root_user.get("name")

    # Step 2: Traverse hierarchy
    while queue:
        current_id, current_level = queue.pop(0)

        if current_id in seen_ids:
            continue
        seen_ids.add(current_id)

        direct_reports = fetch_direct_reports(current_id, token)

        for report in direct_reports:
            report_id = report.get("id")
            if not report_id:
                continue

            report["Level"] = current_level + 1
            all_users.append(report)
            manager_lookup[report_id] = report.get("name")
            queue.append((report_id, current_level + 1))

    # Step 3: Normalize and map ManagerName
    df = normalize_user_data(all_users)

    if not df.empty and "ManagerID" in df.columns:
        df["ManagerName"] = df["ManagerID"].map(manager_lookup)

    return df


def main():
    token = os.getenv("API_TOKEN")
    if not token:
        raise ValueError("Missing API_TOKEN environment variable.")

    os.makedirs(EXPORT_FOLDER, exist_ok=True)

    final_df = crawl_org_with_levels(STARTING_USER_IDS, token)

    if final_df.empty:
        print("No data returned.")
        return

    # Preview key columns
    preview_cols = [col for col in ["name", "title", "ManagerName", "Level"] if col in final_df.columns]
    print(final_df[preview_cols].head(20))

    # Create dynamic filename with date
    today_str = date.today().isoformat()
    filename = f"org_chart_basic_{today_str}.csv"
    export_path = os.path.join(EXPORT_FOLDER, filename)

    # Export to CSV
    final_df.to_csv(export_path, index=False)
    print(f"File saved: {export_path}")


if __name__ == "__main__":
    main()
