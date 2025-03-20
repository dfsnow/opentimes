+++
title = "About"
+++

# What is OpenTimes?

OpenTimes is a database of pre-computed, point-to-point travel times between
United States Census geographies. It lets you download bulk travel time data
for free and with no limits.

All times are calculated using open-source software from publicly available
data. The OpenTimes data pipelines, infrastructure, packages, and website
are all open-source and available on [GitHub](https://github.com/dfsnow/opentimes).
See the [README](https://github.com/dfsnow/opentimes?tab=readme-ov-file#getting-the-data)
to learn how to download the data.

### Goals

The primary goal of OpenTimes is to enable research by providing easily accessible
and free bulk travel time data _between Census geographies_. The target audience
includes academics, urban planners, and anyone who needs to quantify spatial
access to resources (e.g., how many parks someone can reach in an hour).

The secondary goal of OpenTimes is to provide a free alternative to paid
travel time/distance matrix products such as
[Google's Distance Matrix API](https://developers.google.com/maps/documentation/distance-matrix/overview),
[Esri's Network Analyst](https://www.esri.com/en-us/arcgis/products/arcgis-network-analyst/overview) tool,
and [TravelTime](https://traveltime.com). However, note that OpenTimes is not
exactly analogous to these services, which are often doing different and/or more
sophisticated things (e.g. incorporating traffic, leveraging historical times,
performing live routing, etc.).

---

## FAQs

This section focuses on the what, why, and how of the OpenTimes project. For
more specific questions about the data (i.e. its coverage, structure,
and limitations), see the project [README](https://github.com/dfsnow/opentimes?tab=readme-ov-file#opentimes).

<details>
<summary>General questions</summary>

#### What is a travel time?

In this case, a travel time is just how long it takes to get from location A
to location B while following a road or path network. Think Google Maps or
your favorite smartphone mapping service. OpenTimes provides billions of these
times, all pre-calculated from public data. However, unlike a smartphone,
OpenTimes does not provide the route itself, only the time between the two
points.

#### What are the times between?

Times are between the _population-weighted_ centroids of United States Census
geographies. Centroids are weighted because sometimes Census geographies are
huge and their unweighted centroid is in the middle of a desert or mountain
range. However, most people don't want to go to the desert, they want to go
to where other people are. Weighting the centroids moves them closer to where
people actually want to go (i.e. towns and cities).

#### What travel modes are included?

Currently, driving, walking, and biking are included. I plan to add transit
once [Valhalla](https://github.com/valhalla/valhalla) (an alternative to the main
OSRM routing engine OpenTimes uses) adds multi-modal costing to their Matrix API.

#### Are the travel times accurate?

Kind of. They're accurate relative to the other times in this database
(i.e. they are internally consistent), but may not align perfectly with
real-world travel times. Driving times tend to be especially optimistic
(faster than the real world). My hope is to continually improve the accuracy
of the times through successive versions.

#### Why are the driving times so optimistic?

Currently, driving times do not include traffic. This has a large effect in
cities, where traffic greatly influences driving times. Times there tend to be
at least 10-15 minutes too fast. It has a much smaller effect on highways and
in more rural areas. Traffic data isn't included because it's pretty expensive
and adding it might limit the open-source nature of the project.

#### The time between A and B is wrong! How can I get it fixed?

Please file a [GitHub issue](https://github.com/dfsnow/opentimes/issues).
However, understand that given the scale of the project (billions of
times), the priority will always be on fixing systemic issues in the data
rather than fixing individual times.

</details>

<details>
<summary>Technology</summary>

For a more in-depth technical overview of the project, visit the OpenTimes
[GitHub](https://github.com/dfsnow/opentimes) page.

#### What input data is used?

OpenTimes currently uses two major data inputs:

1. OpenStreetMap data. Specifically, the yearly
  North America extracts from
  [Geofabrik](https://download.geofabrik.de/north-america.html#).
2. Census data. Specifically,
  [U.S. Census TIGER/Line](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
  shapefiles, which are used to construct origin and destination points.

Input and intermediate data are built and cached by [DVC](https://dvc.org).
The total size of all input and intermediate data is around 300 GB.

#### How do you calculate the travel times?

All travel time calculations require some sort of routing software to
determine the optimal path between two locations. OpenTimes uses
[Open Source Routing Machine (OSRM)](https://project-osrm.org) because it's
the only routing engine that can generate continent-scale distance matrices at
a reasonable speed (Valhalla and R5 are too slow).

U.S. states are used as the unit of work. For each state, I load all the input
data (road network, points, etc.) for the state plus a 300km buffer around
it. I then use the OSRM
[Table API](https://project-osrm.org/docs/v5.5.1/api/#table-service)
to route from each origin in the state to all destinations in the state
plus the buffer area.

#### What do you use for compute?

Travel times are notoriously compute-intensive to calculate at scale, since
they basically require running a shortest path algorithm many times over a
huge network. However, travel time calculations are also fairly easy to
parallelize, since each origin can be its own discrete job.

I use GitHub Actions to parallelize the calculations by
creating a
[job for each state and year](https://github.com/dfsnow/opentimes/actions/runs/13094249792).
This works surprisingly well and lets me calculate tract-level times for the
entire U.S. in about 12 hours.

#### How is the data served?

Data is served via Parquet files sitting in a public Cloudflare R2 bucket. You
can access a list of all the files [here](https://data.opentimes.org).
Files can be downloaded directly, queried with DuckDB or Arrow, or accessed
via the (forthcoming) R or Python wrapper packages.

To learn more about how to access the data, see the
[GitHub README](https://github.com/dfsnow/opentimes?tab=readme-ov-file#opentimes).

#### How much does this all cost to host?

It's surprisingly cheap. Basically the only cost is
[R2 storage](https://www.cloudflare.com/developer-platform/r2/) from
Cloudflare. Right now, total costs are under $15 per month.

#### What map stack do you use for the homepage?

The homepage uses [Maplibre GL JS](https://github.com/maplibre/maplibre-gl-js)
to show maps. The basemap is [OpenFreeMap's](https://openfreemap.org) Positron.
The tract-level boundaries are
[TIGER/Line](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
cartographic boundaries converted to [PMTiles](https://github.com/protomaps/PMTiles)
using [Tippecanoe](https://github.com/felt/tippecanoe) and hosted on R2.

When you click the map, your browser queries the Parquet files on the public
bucket using [hyparquet](https://github.com/hyparam/hyparquet). It then updates
the map fill using the returned destination IDs and times.

#### Why is the homepage slow sometimes?

The big Parquet files that it queries are supposed to be cached by Cloudflare's
CDN. However, Cloudflare doesn't like large files sitting in its caches,
so the files are constantly getting evicted.

If you click the map and it's slow, it's likely that you're hitting a cold cache.
Click again and it should be much faster. Each state has its own file, so
if you're switching between states you're more likely to encounter a cold cache.

#### How is this project funded?

It's not. I pay out of pocket for the (small) hosting costs.

</details>

<details>
<summary>Usage</summary>

#### Is commercial usage allowed?

Yes, go for it.

#### Are there any usage limits?

No. However, note that the data is hosted by
[Cloudflare](https://www.cloudflare.com), which may impose its own limits if
it determines you're acting maliciously.

#### How do I cite this data?

Attribution is required when using OpenTimes data.

Please see the
[CITATION file on GitHub](https://github.com/dfsnow/opentimes/blob/master/CITATION.cff).
You can also generate APA and BibTeX citations directly from the
project's [GitHub page](https://github.com/dfsnow/opentimes?tab=readme-ov-file#opentimes).

#### What license do you use?

OpenTimes uses the [MIT](https://www.tldrlegal.com/license/mit-license) license.
Input data is from [OpenStreetMap](https://www.openstreetmap.org) and the
[U.S. Census](https://www.census.gov). The basemap on the homepage is
from [OpenFreeMap](https://openfreemap.org). Times are calculated using
[OSRM](https://project-osrm.org).

</details>

---

## Colophon

### Who is behind this project?

I'm Dan Snow, a data scientist/policy wonk currently living in San Francisco.
My blog is at [sno.ws](https://sno.ws).

I spent some time during graduate school as an RA at the
[Center for Spatial Data Science](https://spatial.uchicago.edu), where I helped
calculate lots of travel times. OpenTimes is sort of the spiritual successor to
that work.

I built most of OpenTimes during a six-week programming retreat at the
[Recurse Center](https://www.recurse.com/scout/click?t=e5f3c6558aa58965ec2efe48b1b486af),
which I highly recommend. If you need to contact me about this project, please
reach out [via email](mailto:info@opentimes.org).

### Why did you build this?

A few reasons:

- Bulk travel times are really useful for quantifying access to amenities. In
  academia, they're used to measure spatial access to
  [primary care](https://sno.ws/rural-docs/),
  [abortion](https://www.nytimes.com/interactive/2019/05/31/us/abortion-clinics-map.html),
  and [grocery stores](https://doi.org/10.1186/1476-072X-8-9). In industry,
  they're used to construct [indices for urban amenity access](https://www.walkscore.com)
  and as features for predictive models for real estate prices.
- There's a gap in the open-source spatial ecosystem. The number of open-source
  routing engines, spatial analysis tools, and web mapping libraries has exploded
  in the last decade, but bulk travel times are still difficult to get and/or expensive.
- It's a fun technical challenge to calculate and serve billions of records.
- I was inspired by the [OpenFreeMap](https://openfreemap.org) project and
  wanted to use my own domain knowledge to do something similar.
