This repository uses a custom JAR of `R5` compiled with `gradle shadowJar` from tag `v7.1`.
It is used in this repository for routing instead of the official Conveyal
distribution. It has three modifications:

- The value of [MIN_SUBGRAPH_SIZE](https://github.com/conveyal/r5/blob/e5a9a2653ce8ac561c7b182b87764f2c94e7d594/src/main/java/com/conveyal/r5/streets/StreetLayer.java#L91) is increased from 40 to 100.
- The value of [MAX_BOUNDING_BOX_AREA_SQ_KM](https://github.com/conveyal/r5/blob/e5a9a2653ce8ac561c7b182b87764f2c94e7d594/src/main/java/com/conveyal/r5/common/GeometryUtils.java#L27) is increased from to 975,000 to 50,975,000.
- The values of [LINK_RADIUS_METERS](https://github.com/conveyal/r5/blob/e5a9a2653ce8ac561c7b182b87764f2c94e7d594/src/main/java/com/conveyal/r5/streets/StreetLayer.java#L107) and [INITIAL_LINK_RADIUS_METERS](https://github.com/conveyal/r5/blob/e5a9a2653ce8ac561c7b182b87764f2c94e7d594/src/main/java/com/conveyal/r5/streets/StreetLayer.java#L115) are increased to 250,000 and 15,000, respectively.

The JAR is uploaded to Cloudflare's R2 storage service for use in CI using the follow command:

```
aws s3 mv jars/r5-custom.jar s3://opentimes-resources/jars/ \
    --endpoint-url https://fcb279b22cfe4c98f903ad8f9e7ccbb2.r2.cloudflarestorage.com \
    --profile cloudflare
```
