import jinja2

if __name__ == "__main__":
    N = 8

    with open("dag_spark.jinja2") as f:
        template = jinja2.Template(f.read())

    # Define the data to be used in the template
    data = {"items": ["apple", "banana", "orange"]}

    with open("../pipeline.py") as f:
        pipeline_script = f.read()

    for i in range(1, N + 1):
        output = template.render(
            {
                "dag_index": str(i),
                "pipeline_script": pipeline_script,
                "indices": [str(j) for j in range(i)],
            }
        )
        with open(f"dag_spark_{i}.py", "w") as f:
            f.write(output)
