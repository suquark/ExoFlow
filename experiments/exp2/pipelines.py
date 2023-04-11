import ray
from ray import workflow
from ray.dag import InputNode
from ray.workflow.api import register_service
from ray.workflow.lambda_executor import ray_invoke_lambda


@ray.remote
def idle():
    pass


def beldi_encoder(**outputs):
    if outputs.get("Status") != "Success":
        raise ValueError(f"Task failed with message: {outputs}")
    return outputs["Output"]


def beldi_decoder(**inputs):
    caller_name = inputs.pop("caller_name", "")
    caller_id = inputs.pop("caller_id", "")
    caller_step = inputs.pop("caller_step", 0)
    instance_id = inputs.pop("instance_id", "")
    txn_id = inputs.pop("txn_id", "")
    inst = inputs.pop("instruction", "")
    embed_input = inputs.pop("embed_input", {})
    inputs.update(embed_input)
    return {
        "CallerName": caller_name,
        "CallerId": caller_id,
        "CallerStep": caller_step,
        "InstanceId": instance_id,
        "Input": inputs,
        "TxnId": txn_id,
        "Instruction": inst,
        "Async": False,
    }


def search_encoder(**outputs):
    rates = beldi_encoder(**outputs)
    rate_plans = rates["RatePlans"]
    if rate_plans is None:
        return {"search": {"HotelIds": []}}
    return {"search": {"HotelIds": [r["HotelId"] for r in rate_plans]}}


def register_dags(worker_id: int):
    prefix = "beldi-wf-dev-"
    skip_ckpt = workflow.options(checkpoint=False)
    retry = {"retry_exceptions": True, "max_retries": 100}

    with InputNode() as dag_input:
        geo_output = ray_invoke_lambda.options(**skip_ckpt, **retry).bind(
            prefix + "geo",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            Lat=dag_input.Lat,
            Lon=dag_input.Lon,
        )
        rate_output = ray_invoke_lambda.options(**skip_ckpt, **retry).bind(
            prefix + "rate",
            encoder=search_encoder,
            decoder=beldi_decoder,
            embed_input=geo_output,
            Indate=dag_input.InDate,
            Outdate=dag_input.OutDate,
        )
        register_service(rate_output, workflow_id=f"search_{worker_id}")

    with InputNode() as dag_input:
        # NOTE: recommendation, not recommend in Beldi
        dag = ray_invoke_lambda.options(**skip_ckpt, **retry).bind(
            prefix + "recommendation",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            Require=dag_input.Require,
            Lat=dag_input.Lat,
            Lon=dag_input.Lon,
        )
        register_service(dag, workflow_id=f"recommend_{worker_id}")

    with InputNode() as dag_input:
        dag = ray_invoke_lambda.options(**skip_ckpt, **retry).bind(
            prefix + "user",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            Username=dag_input.Username,
            Password=dag_input.Password,
        )
        register_service(dag, workflow_id=f"user_{worker_id}")

    # parallel reserve
    with InputNode() as dag_input:
        hotel_output = ray_invoke_lambda.options(**retry).bind(
            prefix + "hotel",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            txn_id=dag_input.instance_id,
            userId=dag_input.userId,
            hotelId=dag_input.hotelId,
        )
        flight_output = ray_invoke_lambda.options(**retry).bind(
            prefix + "flight",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            txn_id=dag_input.instance_id,
            userId=dag_input.userId,
            flightId=dag_input.flightId,
        )
        dag = ray_invoke_lambda.options(**retry).bind(
            prefix + "order",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            reserveHotel=hotel_output,
            reserveFlight=flight_output,
            userId=dag_input.userId,
            flightId=dag_input.flightId,
            hotelId=dag_input.hotelId,
        )
        register_service(dag, workflow_id=f"reserve_{worker_id}")

    # serial reserve
    with InputNode() as dag_input:
        hotel_output = ray_invoke_lambda.options(**retry).bind(
            prefix + "hotel",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            txn_id=dag_input.instance_id,
            userId=dag_input.userId,
            hotelId=dag_input.hotelId,
        )
        flight_output = ray_invoke_lambda.options(**retry).bind(
            prefix + "flight",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            txn_id=dag_input.instance_id,
            userId=dag_input.userId,
            flightId=dag_input.flightId,
        )
        hotel_output >> flight_output
        dag = ray_invoke_lambda.options(**retry).bind(
            prefix + "order",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            reserveHotel=hotel_output,
            reserveFlight=flight_output,
            userId=dag_input.userId,
            flightId=dag_input.flightId,
            hotelId=dag_input.hotelId,
        )
        register_service(dag, workflow_id=f"reserve_serial_{worker_id}")

    # reserve and skip checkpoint
    with InputNode() as dag_input:
        hotel_output = ray_invoke_lambda.options(**retry).bind(
            prefix + "hotel",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            txn_id=dag_input.instance_id,
            userId=dag_input.userId,
            hotelId=dag_input.hotelId,
        )
        flight_output = ray_invoke_lambda.options(**retry).bind(
            prefix + "flight",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            txn_id=dag_input.instance_id,
            userId=dag_input.userId,
            flightId=dag_input.flightId,
        )
        dag = ray_invoke_lambda.options(**retry, **skip_ckpt).bind(
            prefix + "order",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            reserveHotel=hotel_output,
            reserveFlight=flight_output,
            userId=dag_input.userId,
            flightId=dag_input.flightId,
            hotelId=dag_input.hotelId,
        )
        register_service(dag, workflow_id=f"reserve_skipckpt_{worker_id}")

    with InputNode() as dag_input:
        hotel_acquire = ray_invoke_lambda.options(
            **retry, **workflow.options(checkpoint="async")
        ).bind(
            prefix + "hotel-acquire",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            txn_id=dag_input.instance_id,
            userId=dag_input.userId,
            hotelId=dag_input.hotelId,
        )
        flight_acquire = ray_invoke_lambda.options(
            **retry, **workflow.options(checkpoint="async")
        ).bind(
            prefix + "flight-acquire",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            txn_id=dag_input.instance_id,
            userId=dag_input.userId,
            flightId=dag_input.flightId,
        )
        hotel_reserve = ray_invoke_lambda.options(**retry, **skip_ckpt).bind(
            prefix + "hotel-reserve",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            acquired=hotel_acquire,
            userId=dag_input.userId,
            hotelId=dag_input.hotelId,
        )
        flight_reserve = ray_invoke_lambda.options(**retry, **skip_ckpt).bind(
            prefix + "flight-reserve",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            acquired=flight_acquire,
            userId=dag_input.userId,
            flightId=dag_input.flightId,
        )
        dag = ray_invoke_lambda.options(
            **retry,
            **workflow.options(
                checkpoint=False,
                wait_until_committed=[
                    prefix + "hotel-acquire",
                    prefix + "flight-acquire",
                ],
            ),
        ).bind(
            prefix + "order",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            reserveHotel=hotel_reserve,
            reserveFlight=flight_reserve,
            userId=dag_input.userId,
            flightId=dag_input.flightId,
            hotelId=dag_input.hotelId,
        )
        register_service(dag, workflow_id=f"reserve_overlapckpt_{worker_id}")

    with InputNode() as dag_input:
        hotel_acquire = ray_invoke_lambda.options(**retry).bind(
            prefix + "hotel-acquire",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            txn_id=dag_input.instance_id,
            userId=dag_input.userId,
            hotelId=dag_input.hotelId,
        )
        flight_acquire = ray_invoke_lambda.options(**retry).bind(
            prefix + "flight-acquire",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            txn_id=dag_input.instance_id,
            userId=dag_input.userId,
            flightId=dag_input.flightId,
        )
        hotel_reserve = ray_invoke_lambda.options(**retry, **skip_ckpt).bind(
            prefix + "hotel-reserve",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            acquired=hotel_acquire,
            userId=dag_input.userId,
            hotelId=dag_input.hotelId,
        )
        flight_reserve = ray_invoke_lambda.options(**retry, **skip_ckpt).bind(
            prefix + "flight-reserve",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            acquired=flight_acquire,
            userId=dag_input.userId,
            flightId=dag_input.flightId,
        )
        dag = ray_invoke_lambda.options(**retry, **skip_ckpt).bind(
            prefix + "order",
            encoder=beldi_encoder,
            decoder=beldi_decoder,
            reserveHotel=hotel_reserve,
            reserveFlight=flight_reserve,
            userId=dag_input.userId,
            flightId=dag_input.flightId,
            hotelId=dag_input.hotelId,
        )
        register_service(dag, workflow_id=f"reserve_nooverlapckpt_{worker_id}")

    register_service(idle.bind(), workflow_id=f"idle_{worker_id}")
