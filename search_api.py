import textwrap

import uvicorn
from clickhouse_driver import Client
from fastapi import FastAPI, HTTPException

app = FastAPI()


def get_similar_words(query, results_count=10):
    client = Client(host="localhost", port=9000)
    results = client.execute(
        textwrap.dedent(
            f"""\
            SELECT
                words,
                ngramDistanceUTF8(words, '{query}') AS distance,
                count() AS occurrence
            FROM default.products
            GROUP BY words
            ORDER BY distance ASC, occurrence DESC, words ASC
            LIMIT {results_count}
            """
        )
    )
    return [row[0] for row in results]


@app.get("/search")
def search(q: str):
    if not q:
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required")
    top_words = get_similar_words(q)
    return top_words


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
