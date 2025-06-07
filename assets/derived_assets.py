from dagster import asset, MetadataValue, TableSchema, TableColumn, AssetKey, AssetExecutionContext
from dagster_duckdb import DuckDBResource

@asset(
    group_name="Analytics",
    compute_kind="duckdb",
    description="Summarizes the number of comments each user has made on issues and pull requests.",
    tags={"marketing":"", "analytics":"", "comments":"", "pull_requests":"", "issues":""},
    deps=["dlt_github_reactions_issues", "dlt_github_reactions_pull_requests" ],
    metadata={
        "dagster/column_schema": TableSchema(
            columns=[
                TableColumn(
                    name="username",
                    type="text",
                    description="GitHub username of the comment author"
                ),
                TableColumn(
                    name="total_comments",
                    type="bigint",
                    description="Total number of comments made by the author"
                ),
                TableColumn(
                    name="type",
                    type="text",
                    description="Whether the comments are from issues or pull requests"
                ),
            ]
        )
    }
)
def top_comment_authors(duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE gh_data.top_comment_authors AS
            SELECT
                author__login AS username,
                COUNT(*) AS total_comments,
                'issue' AS type
            FROM gh_data.issues__comments
            GROUP BY author__login

            UNION ALL

            SELECT
                author__login AS username,
                COUNT(*) AS total_comments,
                'pull_request' AS type
            FROM gh_data.pull_requests__comments
            GROUP BY author__login
            ORDER BY total_comments DESC;

        """)


@asset(
    group_name="Analytics",
    compute_kind="duckdb",
    tags={"marketing":"", "analytics":"", "comments":"", "pull_requests":"", "issues":""},
    description="Shows the number of comments made per day for issues and pull requests.",
    deps=["dlt_github_reactions_issues", "dlt_github_reactions_pull_requests" ],
    metadata={
        "dagster/column_schema": TableSchema(
            columns=[
                TableColumn(
                    name="day",
                    type="date",
                    description="The day the comments were made"
                ),
                TableColumn(
                    name="num_comments",
                    type="bigint",
                    description="Number of comments on that day"
                ),
                TableColumn(
                    name="type",
                    type="text",
                    description="Whether the comment came from an issue or pull request"
                ),
            ]
        )
    }
)
def comment_volume_by_day(duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE gh_data.comment_volume_by_day AS
            SELECT
                DATE_TRUNC('day', created_at) AS day,
                COUNT(*) AS num_comments,
                'issue' AS type
            FROM gh_data.issues__comments
            GROUP BY day

            UNION ALL

            SELECT
                DATE_TRUNC('day', created_at) AS day,
                COUNT(*) AS num_comments,
                'pull_request' AS type
            FROM gh_data.pull_requests__comments
            GROUP BY day
            ORDER BY day ASC;

        """)

        
@asset(
    group_name="Analytics",
    compute_kind="duckdb",
    tags={"marketing":"", "analytics":"", "comments":"", "pull_requests":"", "issues":""},
    description="Calculates the average length of issue and pull request comments.",
    deps=["dlt_github_reactions_issues", "dlt_github_reactions_pull_requests" ],
    metadata={
        "dagster/column_schema": TableSchema(
            columns=[
                TableColumn(
                    name="type",
                    type="text",
                    description="Whether the comment came from an issue or pull request"
                ),
                TableColumn(
                    name="avg_length",
                    type="double",
                    description="Average character length of the comment body"
                ),
            ]
        )
    }
)
def avg_comment_length(duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE gh_data.avg_comment_length AS
            SELECT
                'issue' AS type,
                AVG(LENGTH(body)) AS avg_length
            FROM gh_data.issues__comments

            UNION ALL

            SELECT
                'pull_request' AS type,
                AVG(LENGTH(body)) AS avg_length
            FROM gh_data.pull_requests__comments;

        """)



@asset(
    group_name="Analytics",
    compute_kind="duckdb",
    tags={"marketing":"", "analytics":"", "response_time":"", "resolution_time":"", "CSAT":""},
    description="Summarizes each team's average response time, resolution time, and CSAT rating based on ticket data.",
    deps=[AssetKey(["target", "public", "tickets"])],
    metadata={
        "dagster/column_schema": TableSchema(
            columns=[
                TableColumn(
                    name="team_name",
                    type="text",
                    description="Name of the team handling the tickets"
                ),
                TableColumn(
                    name="total_tickets",
                    type="bigint",
                    description="Total number of tickets handled by the team"
                ),
                TableColumn(
                    name="avg_first_reply_secs",
                    type="double",
                    description="Average time in seconds to first reply"
                ),
                TableColumn(
                    name="avg_resolution_secs",
                    type="double",
                    description="Average time in seconds to fully resolve the ticket"
                ),
                TableColumn(
                    name="avg_csat",
                    type="double",
                    description="Average customer satisfaction rating"
                ),
            ]
        )
    }
)
def team_performance_summary(context: AssetExecutionContext,duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        conn.execute("""
            CREATE OR REPLACE TABLE analytics.team_performance_summary AS
                SELECT
                    t.team_name,
                    COUNT(*) AS total_tickets,
                    AVG(t.seconds_to_first_reply) AS avg_first_reply_secs,
                    AVG(t.seconds_to_last_close) AS avg_resolution_secs,
                    AVG(t.csat_rating) AS avg_csat
                FROM main.public_tickets t
                GROUP BY t.team_name
                ORDER BY avg_csat DESC;

        """)

        # Row count and metadata for Dagster UI
        row_count = conn.execute("SELECT COUNT(*) FROM analytics.team_performance_summary").fetchone()[0]
        table_metadata = conn.execute(
            "SELECT * FROM duckdb_tables() WHERE table_name = 'team_performance_summary' AND schema_name = 'analytics'"
        ).pl()

    context.add_output_metadata({
        "num_rows": row_count,
        "table_name": table_metadata['table_name'][0],
        "schema_name": table_metadata['schema_name'][0],
        "database_name": table_metadata['database_name'][0],
        "column_count": table_metadata['column_count'][0],
        "estimated_size": table_metadata['estimated_size'][0],
    })        

@asset(
    group_name="Analytics",
    compute_kind="duckdb",
    tags={"marketing":"", "analytics":"", "nps":"", "user_responses":"", "months":"12"},
    description="NPS score by months subscribed, based on user responses, capped at first 12 months.",
    deps=[AssetKey(["target", "public", "nps_responses"])],
    metadata={
        "dagster/column_schema": TableSchema(columns=[
            TableColumn("months_subscribed", "int", "Number of months the user was subscribed"),
            TableColumn("NPS", "float", "Net Promoter Score for users at that subscription duration"),
        ])
    }
)
def nps_by_subscription_month(context: AssetExecutionContext, duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        conn.execute("""
            CREATE OR REPLACE TABLE analytics.nps_by_subscription_month AS
                WITH nps_calcs AS (
                    SELECT
                        months_subscribed::integer AS months_subscribed,
                        COUNT(1) FILTER (WHERE nps_response >= 9)::float AS promoter_ct,
                        COUNT(1) FILTER (WHERE nps_response <= 6)::float AS detractor_ct,
                        COUNT(1)::float AS all_responses_ct
                    FROM main.public_nps_responses
                    GROUP BY 1
                    ORDER BY 1
                )
                SELECT
                    months_subscribed,
                    ((promoter_ct / all_responses_ct) - (detractor_ct / all_responses_ct)) * 100.00 AS NPS
                FROM nps_calcs
                ORDER BY 1
                LIMIT 12;
        """)

@asset(
    group_name="Analytics",
    compute_kind="duckdb",
    tags={"marketing":"", "analytics":"", "user_activity":"", "tickets":"", "events":"", "teams":""},
    description="Comprehensive user activity analytics derived from users, tickets, events, and teams.",
    deps=[
        AssetKey(["target", "public", "users"]),
        AssetKey(["target", "public", "tickets"]),
        AssetKey(["target", "public", "teams"]),
        AssetKey(["target", "public", "events"]),
    ],
    metadata={
        "dagster/column_schema": TableSchema(columns=[
            TableColumn("user_id", "int", "User ID"),
            TableColumn("user_email", "varchar", "User email"),
            TableColumn("ticket_id", "int", "Ticket ID"),
            TableColumn("ticket_status", "varchar", "Ticket status"),
            TableColumn("ticket_created", "timestamp", "Ticket creation time"),
            TableColumn("team_name", "varchar", "User's team name"),
            TableColumn("event_id", "uuid", "Event ID"),
            TableColumn("event_time", "timestamp", "Time the event occurred"),
            TableColumn("event_name", "varchar", "Type of event"),
        ])
    }
)
def user_core_activity_summary(context: AssetExecutionContext, duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        conn.execute("""
            CREATE OR REPLACE TABLE analytics.user_core_activity_summary AS
            SELECT
                u.id AS user_id,
                u.email AS user_email,
                t.id AS ticket_id,
                t.status AS ticket_status,
                t.created_at AS ticket_created,
                tm.name AS team_name,
                e.id AS event_id,
                e.time AS event_time,
                e.name AS event_name
            FROM main.public_users u
            LEFT JOIN main.public_tickets t ON u.id = t.user_id
            LEFT JOIN main.public_teams tm ON u.team_id = tm.id
            LEFT JOIN main.public_events e ON u.id = e.user_id
        """)


derived_analytics_assets = [
    top_comment_authors,
    comment_volume_by_day,
    avg_comment_length,
    team_performance_summary,
    nps_by_subscription_month,
    user_core_activity_summary
]