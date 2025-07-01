# utils
def get_i_col(string_id: str) ->str:
    return f"I_{string_id}"


# Dim Reduction config
ZERO_THRESHOLD = 0.1
ZERO_HOUR = 5
ZERO_RATE = 0.1

DOUBLE_RATE = 0.2

UMAP_PARAMS = {
    "n_components": 2,
    "n_neighbors": 15,
    "n_jobs": -1,
    # "random_state": 42
}
