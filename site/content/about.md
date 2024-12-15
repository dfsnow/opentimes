+++
title = "About"
+++

# What is OpenTimes?

OpenTimes lets you download bulk travel data for free and with no limits.
It is a database of around 20 billion point-to-point travel times and
distances between United States Census geographies.

All times are calculated using open-source software from publicly available
data. The OpenTimes data pipelines, infrastructure, packages, and website
are all open-source and available on [GitHub](https://github.com/dfsnow/opentimes).

### Goals

The primary goal of OpenTimes is to enable research by providing an
accessible and free source of bulk travel data. The target audience
is academics, urban planners, or really anyone who needs to quantify spatial
access to resources (i.e. how many grocery stores someone can reach in an hour).

The secondary goal is to provide a free alternative to paid
travel time/distance matrix products such as
[Google's Distance Matrix API](https://developers.google.com/maps/documentation/distance-matrix/overview),
[Esri's Network Analyst](https://www.esri.com/en-us/arcgis/products/arcgis-network-analyst/overview) tool,
and [traveltime.com](https://traveltime.com). However, note that OpenTimes is not
exactly analogous to these services, which are often doing different and/or more
sophisticated things (i.e. incorporating traffic and/or historical times,
performing live routing, etc.).

---

## FAQs

This section focuses on the what, why, and how of the overall project. For
more specific questions about the data (i.e. its coverage, construction, and
limitations), see the [Data]({{< ref "data" >}}) section.

<details>
<summary>General questions</summary>

#### What is a travel time?

In this case, a travel time is just how long it takes to get from location A
to location B when following a road or path network. Think the Google Maps or
your favorite smartphone mapping service. OpenTimes provides billions of these
times, all pre-calculated from public data. It also provides the distance
traveled for each time, though unlike a smartphone map, it does not provide
the route itself.

#### What are the times between?

Times are between the _population-weighted_ centroids of United States Census
geographies. See [Data]({{< ref "data" >}}) for a full list of geographies.
Centroids are weighted because sometimes Census geographies are huge and their
unweighted centroid is in the middle of a desert or mountain range. However,
most people don't want to go to the desert, they want to go to where other
people are. Weighting the centroids moves them closer to where people actually
want to go (i.e. towns and cities).

#### What travel modes are included?

Currently, driving, walking, and biking are included. I plan to include transit
once [Valhalla](https://github.com/valhalla/valhalla) (the routing engine
OpenTimes uses) incorporates multi-modal costing into their matrix API.

#### Are the travel times accurate?

Kind of. They're accurate relative to the other times in this database
(i.e. the are internally consistent), but may not align perfectly with
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

For more a more in-depth technical overview of the project, visit the OpenTimes
[GitHub](https://github.com/dfsnow/opentimes) page.

#### What input data is used?

OpenTimes currently uses three major data inputs:

1. OpenStreetMap data. Specifically, the yearly
  North America extracts from
  [Geofabrik](https://download.geofabrik.de/north-america.html#).
2. Elevation data. Automatically downloaded by
  [Valhalla](https://github.com/valhalla/valhalla). Uses the
  public [Amazon Terrain Tiles](https://registry.opendata.aws/terrain-tiles/).
3. Origin and destination points. Derived from the centroids of
  [U.S. Census TIGER/Line](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
  data.

Input and intermediate data are built and cached by [DVC](https://dvc.org).
The total size of all input and intermediate data is around 750 GB.
In the future, OpenTimes will also use [GTFS data](https://gtfs.org)
for public transit routing.

#### How do you calculate the travel times?

All travel time calculations require some sort of routing engine to
determine the optimal path between two locations. OpenTimes uses
[Valhalla](https://github.com/valhalla/valhalla) because it's fast,
has decent Python bindings, can switch settings on the fly, and has a
low memory/resource footprint.

U.S. states are used as the unit of work. For each state, I load all the input
data (road network, elevation, etc.) for the state plus a 300km buffer around
it. I then use the Valhalla
[Matrix API](https://valhalla.github.io/valhalla/api/matrix/api-reference/)
to route from each origin in the state to all destinations in the state
plus the buffer area.

#### What do you use for compute?

Travel times are notoriously compute-intensive to calculate at scale, since
they basically require running a shortest path algorithm many times over a
very large network. However, they're also fairly easy to parallelize since
each origin can be its own job, independent from the other origins.

I use a combination of GitHub Actions and a beefy home server to calculate
the times for OpenTimes. On GitHub Actions, I use a workflow-per-state model,
where each state runs in a
[parameterized workflow](https://github.com/dfsnow/opentimes/actions/workflows/calculate-times.yaml)
that splits the work into many smaller jobs that run in parallel. This works
surprisingly well and lets me calculate tract-level times for the entire U.S.
in about a day.

#### How is the data served?

Data is served via Parquet files sitting in a public Cloudflare R2 bucket. You
can access a list of all the files [here](https://data.opentimes.org).
Files can be downloaded directly, queried with DuckDB or Arrow, or accessed
via the provided R or Python wrapper packages.

To learn more about how to access the data, see the dedicated
[Data]({{< ref "data" >}}) section.

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

The Parquet files that it queries are supposed to be cached by Cloudflare's
CDN. However, Cloudflare really doesn't seem to like very large files sitting
in its caches, so they're constantly evicting them.

If you click the map and it's slow, it's likely that you're hitting a cold cache.
Click again and it should be much faster. Also, each state has its own file, so
if you're switching between states you're more likely to encounter a cold cache.

#### How is this project funded?

I pay out of pocket for the (currently) small hosting costs. I'm not currently
seeking funding or sponsors, though I may in the future in order to buy
things like traffic data.

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
[GitHub project](https://github.com/dfsnow/opentimes) page.

#### What license do you use?

OpenTimes uses the [MIT](https://www.tldrlegal.com/license/mit-license) license.
Input data is from [OpenStreetMap](https://www.openstreetmap.org) and the
[U.S. Census](https://www.census.gov). The basemap on the homepage is
from [OpenFreeMap](https://openfreemap.org). Times are calculated using
[Valhalla](https://github.com/valhalla/valhalla).

</details>

---

## Colophon

### Who is behind this project?

I'm Dan Snow, a data scientist/policy wonk currently living in Chicago. My
blog is at [sno.ws](https://sno.ws).

I spent some time during graduate school as an RA at the
[Center for Spatial Data Science](https://spatial.uchicago.edu), where I helped
calculate lots of travel times. OpenTimes is sort of the spiritual successor to
that work.

I built OpenTimes during a 6-week programming retreat at the
[Recurse Center](https://www.recurse.com/scout/click?t=e5f3c6558aa58965ec2efe48b1b486af),
which I highly recommend.

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
- I was inspired by the [OpenFreeMap](https://openfreemap.org) project and wanted to use
  my own domain knowledge to do something similar.
