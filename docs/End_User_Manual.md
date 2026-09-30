# End User Manual: NYC 311 Operations Dashboard

This guide is for anyone looking at the dashboard to understand 311 complaint activity across New York City. It doesn't cover how the data was built or processed. It covers how to open the dashboard, read it, and get answers out of it.

## 1. Accessing the Dashboard

The dashboard is published publicly and doesn't require an account, login, or software installation. Open it in any web browser:

**https://public.tableau.com/app/profile/sohail.ali.jafar.ali7660/viz/NYC311ServiceRequestsDashboard2023/NYC311OperationsDashboard**

It works on desktop and most tablets. On a phone screen the charts get cramped, so a laptop or desktop browser is the better experience.

## 2. Dashboard Layout

The dashboard has three charts arranged in a single view:

- **Daily Trend** (top, full width): a line chart showing the total number of 311 requests received each day across 2023.
- **Borough Comparison** (bottom left): a stacked bar chart showing total request volume by borough, broken down by complaint category.
- **Resolution Time by Category** (bottom right): a bar chart showing the average time, in hours, it took to resolve requests in each complaint category.

All three charts are drawn from the same underlying dataset and update together, which is covered in section 4.

## 3. What the Numbers Mean

- **Total Requests**: the count of individual 311 service requests logged in that time period, borough, or category.
- **Avg Resolution Hours**: the average number of hours between when a request was created and when it was marked closed. Requests that were never closed are not included in this average, so a high number reflects genuinely slow-moving categories rather than requests that are simply still pending.
- **Complaint Category**: a grouping of the more specific complaint types (for example, "Noise - Residential" and "Noise - Commercial" both roll up into the "Noise" category) to make the chart readable. Fourteen categories cover the full dataset.
- **Borough**: one of the five NYC boroughs, plus a sixth group labeled "UNSPECIFIED" for the small number of requests with no borough recorded.

## 4. Filtering by Borough

The Borough Comparison chart doubles as a filter for the whole dashboard. Click on any borough's bar (for example, "BROOKLYN") and the Daily Trend line and the Resolution Time bars both update to show only that borough's data. The borough you clicked stays highlighted in full color while the others fade, so you can see what's currently selected at a glance.

To clear the filter and return to citywide totals, click the same bar again, or click on an empty area of the chart.

## 5. Reading the Date Axis

The Daily Trend chart's x-axis runs from January 1 to December 31, 2023, with one point per day. Hovering over any point on the line shows the exact date and the total request count for that day. There's no separate date picker. The whole year is shown at once, so trends across seasons (for example, resolution-time differences between winter and summer) are visible without needing to change any setting.

## 6. Reading the Category Breakdown

In the Borough Comparison chart, each bar is split into colored segments, one per complaint category. The legend beneath the chart maps each color to a category name. Hovering over any individual segment shows the exact category, borough, and request count for that slice, rather than needing to read it off the color alone.

In the Resolution Time chart, categories are already sorted from slowest average resolution time at the top to fastest at the bottom, so the two ends of the chart are the two ends of the spectrum without needing to scan the whole list.

## 7. Interpreting the Visualizations

A few patterns are worth knowing about, since they come directly from the data rather than being an artifact of how the chart is drawn:

- The Daily Trend line has a repeating up-and-down pattern roughly every seven days. That's weekday versus weekend volume, not noise in the data.
- There's a sharp single-day spike in mid-October, nearly double the surrounding days. It's a genuine data point, not an error, though the cause isn't identified in this dashboard.
- Parking & Vehicles and Housing & Buildings dominate total volume almost everywhere, but which one leads shifts by borough. The Bronx in particular swings between Housing & Buildings in colder months and Noise in warmer ones.
- Resolution time varies enormously by category. Parking & Vehicles complaints typically resolve in single-digit hours, while Parks & Trees and Consumer & Business complaints average well over a thousand hours. That gap reflects real differences in how those categories are handled, not a data quality issue.

## 8. Exporting Results

Tableau Public includes built-in export options on any published view:

- **Download the image**: click the small download icon in the bottom toolbar beneath the dashboard, then choose **Image** to save a picture of the current view (including whatever filter is applied).
- **Download the underlying data**: from the same toolbar, choose **Data**, then pick the chart you want data from. This gives a table of the exact numbers behind that chart, which can be copied into a spreadsheet.
- **Download a PDF**: the same toolbar offers a **PDF** option for a printable version of the current view.

Exports reflect whatever filter is currently applied, so if you've clicked a borough to filter the dashboard, the export will be scoped to that borough too. Clear the filter first if you want the citywide export.

## 9. Common Issues

**The dashboard looks cut off or a chart has a scrollbar inside it.** This is usually a browser window sized smaller than the dashboard's layout. Try maximizing the browser window, or use your browser's zoom-out function (Ctrl and minus on Windows) to fit more of the view on screen.

**Clicking a borough doesn't seem to do anything.** Make sure you're clicking directly on a colored bar segment in the Borough Comparison chart, not on the chart's title or axis labels. A successful click will fade the other boroughs and update the two other charts within a second or two.

**The dashboard is slow to load.** It's fetching and rendering data from Tableau Public's servers on first load. A slow initial load is normal, especially on a slower connection; interactions after that (clicking filters, hovering for tooltips) are fast, since the data is already loaded.

## 10. Basic Troubleshooting

| Symptom | Try this |
|---|---|
| Blank page or spinning loader that never finishes | Refresh the page. If it persists, try a different browser or disable browser extensions that block scripts. |
| Numbers look wrong or don't match expectations | Check whether a borough filter is currently applied (see section 4) before assuming the underlying data is incorrect. |
| Tooltips not appearing on hover | Some touchscreens require a tap-and-hold instead of a hover. On desktop, make sure your cursor is directly over a bar or line segment, not the surrounding whitespace. |
| Dashboard looks different from this guide | The published dashboard may have been updated since this guide was written. The core layout (one trend chart, two comparison charts, borough click-filtering) is expected to stay stable. |

If none of the above resolves the issue, the dashboard can be reopened fresh at the link in section 1, which resets any applied filter and reloads the current published version.
