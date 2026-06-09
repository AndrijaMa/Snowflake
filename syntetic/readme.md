The notebook generates synthetic web analytics / clickstream event data and inserts it into a Snowflake Iceberg table (STREAM.PUBLIC.STREAM1).
Here's a breakdown:

Data Structure Each record is a single VARIANT column (DATA) containing a JSON object with these fields:

custom_properties Marketing and experimentation metadata:

country — one of 10 countries (US, FR, DE, GB, JP, BR, IN, CA, AU, MX) experiment_id — random A/B test identifier (e.g. exp_a3f1b2) utm_campaign — marketing campaign type (retargeting, brand_awareness, conversion, loyalty, seasonal) utm_medium — traffic channel (social, email, cpc, organic, referral) utm_source — traffic source (email, facebook, google, twitter, linkedin, instagram) variant — A/B test variant (control, variant_a, variant_b, variant_c) data A random hex string (128 chars) — acts as a payload/placeholder.

device_context Client device information:

browser — Chrome, Firefox, Safari, Edge, Opera browser_version — version 100–125 device_type — desktop, mobile, tablet os — various Windows, macOS, Linux, iOS, Android, ChromeOS versions screen_resolution — common screen sizes user_agent — static placeholder string event_context The behavioral event itself:

event_type — weighted distribution: click (70%), add_to_cart (25%), purchase (5%), plus page_view, form_submit, scroll, sign_up tags — random subset from: trending, recommended, premium, new, featured, popular, sale, limited timestamp — randomized time within the current day (ISO 8601 UTC) identifiers event_id — unique UUID per event session_id — unique UUID per session Scale & Storage 10 million rows per run (configurable via row_count) Stored as an Iceberg v3 table with Snowflake-managed catalog and external volume Warehouse is scaled up to LARGE for the insert, then back to MEDIUM Purpose This simulates a realistic clickstream event stream — useful for testing streaming ingestion pipelines, analytics dashboards, or A/B testing frameworks at scale.
