from __future__ import print_function
import time
from builtins import range
from pprint import pprint
from airflow.models.dagrun import DagRun
from airflow.utils.dates import days_ago
from airflow.contrib.operators.ssh_operator import  SSHOperator 
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

ARGS = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='yolov5_recursize',
    default_args=ARGS,
    schedule_interval=None,
    tags=['example']
)

# Step1
dump_image_from_cvat = SSHOperator(
    task_id="Dump_image_from_cvat",
    ssh_conn_id="cv1",
    command="cd yolov5_training; mkdir -p {{dag_run.conf['cvat_dump_keyword']}}; ./cvat_image_dump.sh {{dag_run.conf['cvat_dump_keyword']}}",
    dag=dag
)

# Step 2. Data preparation
unzip_dumped_image_zips = SSHOperator(
    task_id="unzip_dumped_image_zips",
    ssh_conn_id="cv1",
    command="cd yolov5_training; source ./init_unzip.sh {{dag_run.conf['cvat_dump_keyword']}}",    
    dag=dag
)

data_random = SSHOperator(
    task_id="data_random",
    ssh_conn_id="cv1",
    command="cd yolov5_training; python3 data_random.py",
    get_pyt=True,
    dag=dag
)

HYP_SCRATCH_CONFIGS =["lr0","lrf","momentum","weight_decay","warmup_epochs","warmup_momentum","warmup_bias_lr","box",
"cls","cls_pw","obj","obj_pw","iou_t","anchor_t","fl_gamma","hsv_h","hsv_s","hsv_v","degrees","translate","scale","shear",
"perspective","flipud","fliplr","mosaic","mixup"]


variable_names = Variable.get("variable_names", deserialize_json=True)
# check hyp.scratch.yaml variables to create command in next operator
for variable_name in variable_names["names"]:

    #modify_yamls command
    config = Variable.get(variable_name ,  deserialize_json=True)
    modify_yamls_command = "--save_path {{dag_run.conf['cvat_dump_keyword']}} "
    for key, value in config.items():
        if key in HYP_SCRATCH_CONFIGS:
            _ = f"--{key} {str(value)} "
            modify_yamls_command.join(_)

    modify_yamls = SSHOperator(
        task_id=f"modify_yamls_{variable_name}",
        ssh_conn_id="cv1",
        command=f"cd yolov5_training; python3 modify_yaml.py {modify_yamls_command}",
        get_pyt=True,
        dag=dag
    )


    # docker command 
    host="tcp://172.16.16.88"
    action="run --rm --gpus all --shm-size=16g"
    image="arthurtibame/yolov5:red"
    data_volume="/home/ub/yolov5_training/yolo_data:/usr/src/yolo_data"    
    coco128_yaml_volume="/home/ub/yolov5_training/{{dag_run.conf['cvat_dump_keyword']}}/coco128.yaml:/usr/src/app/data/coco128.yaml"    
    hyp_scratch_yaml_volume="/home/ub/yolov5_training/{{dag_run.conf['cvat_dump_keyword']}}/hyp.scratch.yaml:/usr/src/app/data/hyp.scratch.yaml"    
    
    weight_size = config.get("weight_size") if config.get("weight_size") else "s" 
    yolo_size_yaml_volume="/home/ub/yolov5_training/{{dag_run.conf['cvat_dump_keyword']}}/yolov5" + \
        weight_size + ".yaml:/usr/src/app/models/yolov5"+\
        weight_size + ".yaml"
    
    model_volume = "/home/ub/yolov5_training/{{dag_run.conf['cvat_dump_keyword']}}/runs:/usr/src/app/runs"
    
    
    epochs = config.get("epochs") if config.get("epochs") else "300"
    batch_size = config.get("batch_size") if config.get("batch_size") else "8"

    entry_point=f"python3 train.py --weights yolov5{weight_size}.pt --cfg yolov5{weight_size}.yaml --epochs {epochs} --batch-size {batch_size} --log-imgs 100"        

    cv1_trainer = SSHOperator(
        task_id=f"yolov5_training_{variable_name}",
        ssh_conn_id="cv1",
        command=f"docker -H {host} {action} -v {data_volume} -v {coco128_yaml_volume} -v {hyp_scratch_yaml_volume} -v {yolo_size_yaml_volume} -v {model_volume} {image} {entry_point}",
        dag=dag
    )

## Original command
# "docker -H tcp://172.16.16.88 run --rm --gpus all \
#         -v /home/ub/yolov5_training/yolo_data:/usr/src/yolo_data \
#         -v /home/ub/yolov5_training/{{dag_run.conf['cvat_dump_keyword']}}/coco128.yaml:/usr/src/app/data/coco128.yaml  \
#         -v /home/ub/yolov5_training/{{dag_run.conf['cvat_dump_keyword']}}/hyp.scratch.yaml:/usr/src/app/data/hyp.scratch.yaml \
#         -v /home/ub/yolov5_training/{{dag_run.conf['cvat_dump_keyword']}}/yolov5{{var.json.yolov5_testing.weight_size}}.yaml:/usr/src/app/models/yolov5{{var.json.yolov5_testing.wegiht_size}}.yaml \
#         -v /home/ub/yolov5_training/{{dag_run.conf['cvat_dump_keyword']}}/runs:/usr/src/app/runs \
#         arthurtibame/yolov5:latest \
#         python3 train.py --weights yolov5{{var.json.yolov5_testing.weight_size}}.pt --cfg yolov5{{var.json.yolov5_testing.weight_size}}.yaml --epochs {{var.json.yolov5_testing.epochs}} --batch-size {{var.json.yolov5_testing.batch_size}}",    



# cv1_trainer = DockerOperator(
#     task_id="yolov5_training",
#     docker_url="tcp://172.16.16.88:2375",    
#     image='arthurtibame/yolov5',
#     command='python3 train.py --weights yolov5{{var.json.yolov5_testing.weight_size}}.pt --cfg yolov5{{var.json.yolov5_testing.weight_size}}.yaml --epochs {{var.json.yolov5_testing.epochs}} --batch_size {{var.json.yolov5_testing.batch_size}}',
#     container_name='yolov5_training',
#     volumes=[
#         '/home/ub/yolov5_training/yolo_data:/usr/src/yolo_data', # images
#         '/home/ub/yolov5_training/dm1204/coco128.yaml:/usr/src/app/data/coco128.yaml',#coco128.yaml
#         '/home/ub/yolov5_training/dm1204/hyp.scratch.yaml:/usr/src/app/data/hyp.scratch.yaml', #hyp.scratch.yaml
#         '/home/ub/yolov5_training/dm1204/yolov5{{var.json.yolov5_testing.weight_size}}.yaml:/usr/src/app/models/yolov5{{var.json.yolov5_testing.wegiht_size}}.yaml',
#         '/home/ub/yolov5_training/dm1204/runs:/usr/src/app/runs' # model saved
#     ],
#     auto_remove=True,
#     mem_limit="16g",
#     dag=dag
# )
dump_image_from_cvat >> unzip_dumped_image_zips >>data_random >> modify_yamls >> cv1_trainer

