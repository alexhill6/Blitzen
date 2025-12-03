import psycopg2
import pandas as pd
import credentials_copy
import streamlit as st
import altair as alt


# Create a connection using your credentials
conn = psycopg2.connect(
    host="debprodserver.postgres.database.azure.com",
    dbname=credentials_copy.DB_USER,
    user=credentials_copy.DB_USER,
    password=credentials_copy.DB_PASSWORD,
    sslmode="require"
)

# Requirement 1

# SQL query for number of institutions present by state and institution type
query1 = "SELECT b.CONTROL as Control, a.STABBR as State, COUNT(*) as Institutions, b.YEAR FROM institution_ipeds_info as a JOIN institution_scorecard_info as b ON a.UNITID = b.UNITID GROUP BY b.CONTROL, a.STABBR, b.YEAR ORDER BY b.YEAR, a.STABBR, b.CONTROL"

# put results in pandas dataframe
df1 = pd.read_sql(query1, conn)
# rename columns
df1.columns = ['Institution Type', 'State Abbreviation', "Number of Institutions", "Year"]
# remove null values
df1 = df1.dropna()

# Requirement 2

# SQL query for Average In-State and Average Out-of-State
# tuition by Carnegie Classification Score and State
query2 = "SELECT a.CCBASIC, a.STABBR as State, ROUND(AVG(b.TUITIONFEE_IN), 2) AS tin, ROUND(AVG(b.TUITIONFEE_OUT), 2) AS tout, b.YEAR FROM institution_ipeds_info as a JOIN institution_financial as b ON a.UNITID = b.UNITID GROUP BY CCBASIC, STABBR, b.YEAR ORDER BY STABBR, CCBASIC"
# put results in pandas dataframe
df2 = pd.read_sql(query2, conn)
# rename columns
df2.columns = ['Carnegie Classification', 'State Abbreviation', "Average In-State Tuition and Fees ($)", "Average Out-of-State Tuition and Fees ($)", "Year"]
# remove null values
df2 = df2.dropna()

# Requirement 3

# SQL query for best insitutions by 3 Year Loan Repayment Rate
query3_1 = "SELECT a.instnm as Institution, b.CDR3 as CDR3, b.YEAR FROM institution_ipeds_info as a JOIN institution_financial as b ON a.UNITID = b.UNITID WHERE b.CDR3 IS NOT NULL ORDER BY b.CDR3 ASC LIMIT 10"

# put results in pandas dataframe
df3_1 = pd.read_sql(query3_1, conn)

# SQL query for worst insitutions by 3 Year Loan Repayment Rate
query3_2 = "SELECT a.instnm as Institution, b.CDR3 as CDR3, b.YEAR FROM institution_ipeds_info as a JOIN institution_financial as b ON a.UNITID = b.UNITID WHERE b.CDR3 IS NOT NULL ORDER BY b.CDR3 DESC LIMIT 10"

# put results in pandas dataframe
df3_2 = pd.read_sql(query3_2, conn)

# calculate repayment rate as 1 - default rate
df3_1['cdr3'] = 1 - df3_1['cdr3']
df3_2['cdr3'] = 1 - df3_2['cdr3']

# rename columns
df3_1.columns = ['Institution Name', '3 Year Loan Repayment Rate', "Year"]
df3_2.columns = ['Institution Name', '3 Year Loan Repayment Rate', "Year"]

# remove null values
df3_1 = df3_1.dropna()
df3_2 = df3_2.dropna()

# Requirement 4

# SQL Query to 
query4 = "SELECT a.CCBASIC, b.YEAR, ROUND(AVG(b.CDR3)::numeric, 2) AS cdr3, ROUND(AVG(b.TUITIONFEE_IN), 2) AS tin, ROUND(AVG(b.TUITIONFEE_OUT), 2) AS tout FROM institution_ipeds_info as a JOIN institution_financial as b ON a.UNITID = b.UNITID GROUP BY CCBASIC, b.YEAR ORDER BY b.YEAR, CCBASIC"

df4 = pd.read_sql(query4, conn)
df4['cdr3'] = 1 - df4['cdr3']
df4.columns = ['Carnegie Classification', 'Year', 'Average 3 Year Loan Repayment Rate', "Average In-State Tuition and Fees ($)", "Average Out-of-State Tuition and Fees ($)"]
df4 = df4.reset_index(drop=True)
# df4 = df4.dropna()

st.title("IPEDS and College Scorecard Institution Data")

# --- Global year selector ---
all_years = sorted(df2["Year"].unique())  # df1, df2, df3_1, df3_2 all have Year now
selected_year = st.selectbox("Select Year to Display in Tables", all_years)

# --- Institutions Present ---
st.header(f"Institutions Present in {selected_year}")
st.markdown(f"The following institutions were active in {selected_year}:")

df1_filtered = df1[df1["Year"] == selected_year].drop(columns=["Year"])
st.dataframe(df1_filtered, hide_index=True)

# --- Tuition ---
st.header(f"Tuition by Institution Type in {selected_year}")
st.markdown(f"The following gives a breakdown of Tuition Differences for Institution Types in {selected_year}:")

df2_filtered = df2[df2["Year"] == selected_year].drop(columns=["Year"])
st.dataframe(df2_filtered, hide_index=True)

# --- Loan Repayment ---
st.header("Loan Repayment")
st.markdown(f"The following institutions had the best 3 Year loan repayment rates in {selected_year}:")

df3_1_filtered = df3_1[df3_1["Year"] == selected_year].drop(columns=["Year"])
st.dataframe(df3_1_filtered, hide_index=True)

st.markdown(f"The following institutions had the worst 3 Year loan repayment rates in {selected_year}:")

df3_2_filtered = df3_2[df3_2["Year"] == selected_year].drop(columns=["Year"])
st.dataframe(df3_2_filtered, hide_index=True)

st.header("Change in Institution Tuition and Loan Repayment Rates")
st.markdown("From 2019 to 2022, tuition and loan repayment have shown the following trends by Institution Type:")

# --- Metric selection ---
metric_category = st.selectbox(
    "Select metric category",
    ["Tuition", "Repayment"]
)

if metric_category == "Tuition":
    metric = st.selectbox(
        "Select tuition type",
        ["Average In-State Tuition and Fees ($)", "Average Out-of-State Tuition and Fees ($)"]
    )
else:
    metric = st.selectbox(
        "Select repayment metric",
        ["Average 3 Year Loan Repayment Rate"]
    )

# --- Melted long DF ---
df4_long = df4.melt(
    id_vars=["Year", "Carnegie Classification"],
    value_vars=[
        "Average In-State Tuition and Fees ($)",
        "Average Out-of-State Tuition and Fees ($)",
        "Average 3 Year Loan Repayment Rate"
    ],
    var_name="MetricType",
    value_name="Value"
)

# Force numeric values
df4_long["Value"] = pd.to_numeric(df4_long["Value"], errors="coerce")

# Ensure Carnegie Classification is string
df4_long["Carnegie Classification"] = df4_long["Carnegie Classification"].astype(str)

# --- Carnegie multiselect with working Select All / None ---
cc_options = sorted(df4_long["Carnegie Classification"].unique())
cc_key = "selected_cc"

if cc_key not in st.session_state:
    st.session_state[cc_key] = cc_options

col1, col2 = st.columns(2)
with col1:
    if st.button("Select All"):
        st.session_state[cc_key] = cc_options
with col2:
    if st.button("Select None"):
        st.session_state[cc_key] = []

selected_cc = st.multiselect(
    "Select Carnegie Classifications",
    options=cc_options,
    key=cc_key
)

# --- Filtering ---
df4_filtered = df4_long[df4_long["MetricType"] == metric]
df4_filtered = df4_filtered[df4_filtered["Carnegie Classification"].isin(selected_cc)]

# Create the placeholder BEFORE any early exits
chart_placeholder = st.empty()

# EARLY EXIT: if no rows OR no numeric values
if df4_filtered.empty or df4_filtered["Value"].dropna().empty:
    chart_placeholder.empty()
    st.warning("No data available for the current selection.")
    st.header("Additional Thing 1")
    st.markdown("Additional Thing 1:")
    st.stop()

# --- Compute dynamic y-domain (no padding) ---
valid_vals = df4_filtered["Value"].dropna()
ymin = float(valid_vals.min())
ymax = float(valid_vals.max())

y_encoding = alt.Y(
    "Value:Q",
    title=metric,
    scale=alt.Scale(domain=[ymin, ymax], nice=False)
)

# --- Chart ---
chart4_title = f"{metric} Over Time by Carnegie Classification Score"

chart4 = (
    alt.Chart(df4_filtered, title=chart4_title)
    .mark_line()
    .encode(
        x="Year:O",
        y=y_encoding,
        color="Carnegie Classification:N",
        tooltip=["Year", "Carnegie Classification", "MetricType", "Value"]
    )
)

chart_placeholder.altair_chart(chart4, use_container_width=True)

st.header("Degree Completion Rate")

st.markdown("The following table shows how Completion Rate varies across demographics and states")

query5 = "SELECT b.STABBR as State, ROUND(AVG(a.C150_4)::numeric, 2) as Overall_Completion_Rate, ROUND(AVG(a.C150_4_WHITE)::numeric, 2) as Completion_Rate_White, ROUND(AVG(a.C150_4_BLACK)::numeric, 2) as Completion_Rate_Black, ROUND(AVG(a.C150_4_HISP)::numeric, 2) as Completion_Rate_Hispanic, ROUND(AVG(a.C150_4_ASIAN)::numeric, 2) as Completion_Rate_Asian, ROUND(AVG(a.C150_4_AIAN)::numeric, 2) as Completion_Rate_American_Indian_Alaska_Native, ROUND(AVG(a.C150_4_NHPI)::numeric, 2) as Completion_Rate_Native_Hawaiian_Pacific_Islander, ROUND(AVG(a.C150_4_2MOR)::numeric, 2) as Completion_Rate_Two_or_More_Races, ROUND(AVG(a.C150_4_NRA)::numeric, 2) as Completion_Rate_Nonresident_Alien, ROUND(AVG(a.C150_4_UNKN)::numeric, 2) as Completion_Rate_Unknown, a.YEAR as Year FROM institution_completion as a JOIN institution_ipeds_info as b ON a.UNITID = b.UNITID GROUP BY b.STABBR, a.YEAR ORDER BY a.YEAR, b.STABBR"
df5 = pd.read_sql(query5, conn)
df5_filtered = df5[df5["year"] == selected_year].drop(columns=["year"])
st.dataframe(df5_filtered, hide_index=True)

st.header("SAT Score Data")

st.markdown("The following line graph shows how SAT scores vary over time for the overall test and for the math and verbal sections across different states.")

query6 = "SELECT b.STABBR as State, ROUND(AVG(a.SAT_AVG)::numeric, 0) as SAT_AVG, ROUND(AVG(a.SATVR25)::numeric, 0) as SAT_Verbal_25th_PCT, ROUND(AVG(a.SATVRMID)::numeric, 0) as SAT_Verbal_50th_PCT, ROUND(AVG(a.SATVR75)::numeric, 0) as SAT_Verbal_75th_PCT, ROUND(AVG(a.SATMT25)::numeric, 0) as SAT_Math_25th_PCT, ROUND(AVG(a.SATMTMID)::numeric, 0) as SAT_Math_50th_PCT, ROUND(AVG(a.SATVR25)::numeric, 0) as SAT_Math_75th_PCT, a.YEAR as Year FROM institution_admissions as a JOIN institution_ipeds_info as b ON a.UNITID = b.UNITID GROUP BY b.STABBR, a.YEAR ORDER BY a.YEAR, b.STABBR"
df6 = pd.read_sql(query6, conn)

# --- Melt SAT dataframe ---
df6_long = df6.melt(
    id_vars=["year", "state"],
    value_vars=[
        "sat_avg",
        "sat_verbal_25th_pct",
        "sat_verbal_50th_pct",
        "sat_verbal_75th_pct",
        "sat_math_25th_pct",
        "sat_math_50th_pct",
        "sat_math_75th_pct"
    ],
    var_name="metric_type",
    value_name="value"
)

df6_long["Value"] = pd.to_numeric(df6_long["value"], errors="coerce")
df6_long["State"] = df6_long["state"].astype(str)

# --- State selector ---
sat_states = sorted(df6_long["state"].unique())
selected_sat_state = st.selectbox("Select a State (SAT)", sat_states)

# --- Metric multiselect with Select All / None ---
sat_metrics = sorted(df6_long["metric_type"].unique())
sat_metric_key = "sat_selected_metrics"
if sat_metric_key not in st.session_state:
    st.session_state[sat_metric_key] = sat_metrics

col1, col2 = st.columns(2)
with col1:
    if st.button("Select All SAT Metrics"):
        st.session_state[sat_metric_key] = sat_metrics
with col2:
    if st.button("Select None SAT Metrics"):
        st.session_state[sat_metric_key] = []

selected_sat_metrics = st.multiselect(
    "Select SAT Metrics",
    options=sat_metrics,
    key=sat_metric_key
)

# --- Filtering ---
df6_filtered = df6_long[
    (df6_long["state"] == selected_sat_state) &
    (df6_long["metric_type"].isin(selected_sat_metrics))
]

df6_filtered["Year"] = df6_filtered["year"]
df6_filtered["MetricType"] = df6_filtered["metric_type"]

# --- Chart ---
if df6_filtered.empty or df6_filtered["value"].dropna().empty:
    st.warning("No SAT data available for the current selection.")
else:
    ymin, ymax = df6_filtered["value"].min(), df6_filtered["value"].max()
    y_encoding = alt.Y("value:Q", title="SAT Score", scale=alt.Scale(domain=[ymin, ymax], nice=False))

    chart6 = (
        alt.Chart(df6_filtered, title=f"Average SAT Score Trends Over Time in {selected_sat_state}")
        .mark_line(point=True)
        .encode(
            x="year:O",
            y=y_encoding,
            color="metric_type:N",   # color by metric
            tooltip=["year", "metric_type", "value"]
        )
    )
    st.altair_chart(chart6, use_container_width=True)

st.header("ACT Score Data")

st.markdown("The following line graph shows how ACT scores vary over time for the overall test and for the math and english sections across different states.")

query7 = "SELECT b.STABBR as State, ROUND(AVG(a.ACTCM25)::numeric, 0) as ACT_25th_PCT, ROUND(AVG(a.ACTCMMID)::numeric, 0) as ACT_50th_PCT, ROUND(AVG(a.ACTCM75)::numeric, 0) as ACT_75th_PCT, ROUND(AVG(a.ACTEN25)::numeric, 0) as ACT_English_25th_PCT, ROUND(AVG(a.ACTENMID)::numeric, 0) as ACT_English_50th_PCT, ROUND(AVG(a.ACTEN75)::numeric, 0) as ACT_English_75th_PCT, ROUND(AVG(a.ACTMT25)::numeric, 0) as ACT_Math_25th_PCT, ROUND(AVG(a.ACTMTMID)::numeric, 0) as ACT_Math_50th_PCT, ROUND(AVG(a.ACTMT75)::numeric, 0) as ACT_Math_75th_PCT, a.YEAR as Year FROM institution_admissions as a JOIN institution_ipeds_info as b ON a.UNITID = b.UNITID GROUP BY b.STABBR, a.YEAR ORDER BY a.YEAR, b.STABBR"
df7 = pd.read_sql(query7, conn)

# --- Melt ACT dataframe ---
df7_long = df7.melt(
    id_vars=["year", "state"],
    value_vars=[
        "act_25th_pct",
        "act_50th_pct",
        "act_75th_pct",
        "act_english_25th_pct",
        "act_english_50th_pct",
        "act_english_75th_pct",
        "act_math_25th_pct",
        "act_math_50th_pct",
        "act_math_75th_pct"
    ],
    var_name="metric_type",
    value_name="value"
)

df7_long["Value"] = pd.to_numeric(df7_long["value"], errors="coerce")
df7_long["State"] = df7_long["state"].astype(str)

# --- State selector ---
act_states = sorted(df7_long["state"].unique())
selected_act_state = st.selectbox("Select a State (ACT)", act_states)

# --- Metric multiselect with Select All / None ---
act_metrics = sorted(df7_long["metric_type"].unique())
act_metric_key = "act_selected_metrics"
if act_metric_key not in st.session_state:
    st.session_state[act_metric_key] = act_metrics

col1, col2 = st.columns(2)
with col1:
    if st.button("Select All ACT Metrics"):
        st.session_state[act_metric_key] = act_metrics
with col2:
    if st.button("Select None ACT Metrics"):
        st.session_state[act_metric_key] = []

selected_act_metrics = st.multiselect(
    "Select ACT Metrics",
    options=act_metrics,
    key=act_metric_key
)

# --- Filtering ---
df7_filtered = df7_long[
    (df7_long["state"] == selected_act_state) &
    (df7_long["metric_type"].isin(selected_act_metrics))
]

df7_filtered["Year"] = df7_filtered["year"]
df7_filtered["MetricType"] = df7_filtered["metric_type"]

# --- Chart ---
if df7_filtered.empty or df7_filtered["value"].dropna().empty:
    st.warning("No ACT data available for the current selection.")
else:
    ymin, ymax = df7_filtered["value"].min(), df7_filtered["value"].max()
    y_encoding = alt.Y("value:Q", title="ACT Score", scale=alt.Scale(domain=[ymin, ymax], nice=False))

    chart7 = (
        alt.Chart(df7_filtered, title=f"Average ACT Score Trends Over Time in {selected_act_state}")
        .mark_line(point=True)
        .encode(
            x="year:O",
            y=y_encoding,
            color="metric_type:N",   # color by metric
            tooltip=["year", "metric_type", "value"]
        )
    )
    st.altair_chart(chart7, use_container_width=True)

# ---------------------------------------------------------
# Close the connection
# ---------------------------------------------------------
conn.close()
