This is a custom R5 JAR compiled with `gradle shadowJar` from `r5-v7.2-5-ge5a9a26`.
It is used in this repository for routing instead of the official Conveyal
distribution. It has two modifications:

- The value of [MAX_BOUNDING_BOX_AREA_SQ_KM](https://github.com/conveyal/r5/blob/e5a9a2653ce8ac561c7b182b87764f2c94e7d594/src/main/java/com/conveyal/r5/common/GeometryUtils.java#L27) is increased from to 975,000 to 50,975,000.
- The values of [LINK_RADIUS_METERS](https://github.com/conveyal/r5/blob/e5a9a2653ce8ac561c7b182b87764f2c94e7d594/src/main/java/com/conveyal/r5/streets/StreetLayer.java#L107) and [INITIAL_LINK_RADIUS_METERS](https://github.com/conveyal/r5/blob/e5a9a2653ce8ac561c7b182b87764f2c94e7d594/src/main/java/com/conveyal/r5/streets/StreetLayer.java#L115) are increased to 250,000 and 15,000, respectively.
