import pandas as pd
import pytest
from utils.utils import group_by_column_sets, merge_overlapping_df_list


class TestGroupSetHandlers:
    @pytest.fixture
    def df_small_overlap(self):
        df = pd.DataFrame(
            {
                "a": ["a", "b", "a", "b", "a", "b", "c", "c", "d", "d", "d"],
                "b": [1, 2, 1, 2, 3, 3, 2, 3, 1, 2, 3],
            }
        )
        return ("small_overlap", df)

    @pytest.fixture
    def df_big_overlap(self):
        df = pd.DataFrame(
            {
                "a": ["a", "a", "a", "b", "b", "b", "c", "c", "c"],
                "b": [1, 2, 3, 1, 2, 3, 3, 4, 5],
            }
        )
        return ("big_overlap", df)

    @pytest.mark.parametrize("input", ["df_small_overlap", "df_big_overlap"])
    def test_group_by_column_sets_output(self, input, request):
        data_name, data = request.getfixturevalue(input)
        result = group_by_column_sets(data, "a", "b")

        expected = {
            "small_overlap": [
                pd.DataFrame({"a": ["a"] * 2, "b": [1, 3]}),
                pd.DataFrame({"a": ["b"] * 2 + ["c"] * 2, "b": [2, 3, 2, 3]}),
                pd.DataFrame({"a": ["d"] * 3, "b": [1, 2, 3]}),
            ],
            "big_overlap": [
                pd.DataFrame({"a": ["a"] * 3 + ["b"] * 3, "b": [1, 2, 3] * 2}),
                pd.DataFrame({"a": ["c"] * 3, "b": [3, 4, 5]}),
            ],
        }

        assert len(result) == len(expected[data_name])
        for res, exp in zip(result, expected[data_name]):
            pd.testing.assert_frame_equal(
                res.reset_index(drop=True), exp.reset_index(drop=True)
            )

    @pytest.mark.parametrize(
        "input,threshold", [("df_small_overlap", 0.9), ("df_big_overlap", 0.2)]
    )
    def test_merge_overlapping_df_list_output(self, input, threshold, request):
        data_name, data = request.getfixturevalue(input)
        initial = group_by_column_sets(data, "a", "b")
        result = merge_overlapping_df_list(initial, threshold)

        expected = {
            "small_overlap": [
                pd.DataFrame({"a": ["b", "b", "c", "c"], "b": [2, 3, 2, 3]}),
                pd.DataFrame(
                    {"a": ["d"] * 3 + ["a"] * 2, "b": [1, 2, 3, 1, 3]}
                ),
            ],
            "big_overlap": [
                pd.DataFrame(
                    {
                        "a": ["a"] * 3 + ["b"] * 3 + ["c"] * 3,
                        "b": [1, 2, 3] * 2 + [3, 4, 5],
                    }
                )
            ],
        }

        # Check total number of rows is preserved
        assert sum(len(df) for df in initial) == sum(len(df) for df in result)
        for res, exp in zip(result, expected[data_name]):
            pd.testing.assert_frame_equal(
                res.reset_index(drop=True), exp.reset_index(drop=True)
            )
