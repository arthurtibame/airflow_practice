from os import name
import yaml
import argparse

HYP_SCRATCH_DICT = {
    "lr0": 0.01,
    "lrf": 0.2,
    "momentum": 0.937,
    "weight_decay": 0.0005,
    "warmup_epochs": 3.0,
    "warmup_momentum": 0.8,
    "warmup_bias_lr": 0.1,
    "box": 0.05,
    "cls": 0.5,
    "cls_pw": 1.0,
    "obj": 1.0,
    "obj_pw": 1.0,
    "iou_t": 0.20,
    "anchor_t": 4.0,
    "fl_gamma": 0.0,
    "hsv_h": 0.015,
    "hsv_s": 0.7,
    "hsv_v": 0.4,
    "degrees": 0.0,
    "translate": 0.1,
    "scale": 0.5,
    "shear": 0.0,
    "perspective": 0.0,
    "flipud": 0.0,
    "fliplr": 0.5,
    "mosaic": 1.0,
    "mixup": 0.0,
}

def write_template(nc:int, names:list, saved_path:str, template_path="./templates") -> None:    
    coco128_yaml = yaml.load(open(f"{template_path}/coco128.yaml"))    
    yolov5s = yaml.load(open(f"{template_path}/yolov5s.yaml"))
    yolov5m =  yaml.load(open(f"{template_path}/yolov5m.yaml"))
    yolov5l =  yaml.load(open(f"{template_path}/yolov5l.yaml"))
    yolov5x = yaml.load(open(f"{template_path}/yolov5x.yaml"))
    
    #coco
    coco128_yaml['nc']=nc
    coco128_yaml['names']=names
    #s
    yolov5s['nc']=nc
    #m
    yolov5m['nc']=nc
    #l
    yolov5l['nc']=nc
    #x
    yolov5x['nc']=nc
    with open(f'{saved_path}/coco128.yaml', 'w') as f:    
        data = yaml.dump(coco128_yaml, f)    
    with open(f'{saved_path}/yolov5s.yaml', 'w') as f:
        yaml.dump(yolov5s,f)
    with open(f'{saved_path}/yolov5m.yaml', 'w') as f:
        yaml.dump(yolov5m,f )
    with open(f'{saved_path}/yolov5l.yaml', 'w') as f:
        yaml.dump(yolov5l,f )
    with open(f'{saved_path}/yolov5x.yaml', 'w') as f:
        yaml.dump(yolov5x,f)

def write_hyp_scratch(saved_path, template_path="./templates", hyp_scratch_dict=HYP_SCRATCH_DICT):
    hyp_scratch = yaml.load(open(f"{template_path}/hyp.scratch.yaml"))
    hyp_scratch["lr0"]=hyp_scratch_dict['lr0']
    hyp_scratch["lrf"]=hyp_scratch_dict['lrf']
    hyp_scratch["momentum"]=hyp_scratch_dict['momentum']
    hyp_scratch["weight_decay"]=hyp_scratch_dict['weight_decay']
    hyp_scratch["warmup_epochs"]=hyp_scratch_dict['warmup_epochs']
    hyp_scratch["warmup_momentum"]=hyp_scratch_dict['warmup_momentum']
    hyp_scratch["warmup_bias_lr"]=hyp_scratch_dict['warmup_bias_lr']
    hyp_scratch["box"]=hyp_scratch_dict['box']
    hyp_scratch["cls"]=hyp_scratch_dict['cls']
    hyp_scratch["cls_pw"]=hyp_scratch_dict['cls_pw']
    hyp_scratch["obj"]=hyp_scratch_dict['obj']
    hyp_scratch["obj_pw"]=hyp_scratch_dict['obj_pw']
    hyp_scratch["iou_t"]=hyp_scratch_dict['iou_t']
    hyp_scratch["anchor_t"]=hyp_scratch_dict['anchor_t']
    hyp_scratch["fl_gamma"]=hyp_scratch_dict['fl_gamma']
    hyp_scratch["hsv_h"]=hyp_scratch_dict['hsv_h']
    hyp_scratch["hsv_s"]=hyp_scratch_dict['hsv_s']
    hyp_scratch["hsv_v"]=hyp_scratch_dict['hsv_v']
    hyp_scratch["degrees"]=hyp_scratch_dict['degrees']
    hyp_scratch["translate"]=hyp_scratch_dict['translate']
    hyp_scratch["scale"]=hyp_scratch_dict['scale']
    hyp_scratch["shear"]=hyp_scratch_dict['shear']
    hyp_scratch["perspective"]=hyp_scratch_dict['perspective']
    hyp_scratch["flipud"]=hyp_scratch_dict['flipud']
    hyp_scratch["fliplr"]=hyp_scratch_dict['fliplr']
    hyp_scratch["mosaic"]=hyp_scratch_dict['mosaic']
    hyp_scratch["mixup"]=hyp_scratch_dict['mixup']

    with open(f'{saved_path}/hyp.scratch.yaml', 'w') as f:
        yaml.dump(hyp_scratch, f)
     


def load_obj_names(tmp_path="./tmp") -> tuple:
    with open(f"{tmp_path}/obj.names") as f:
        names = [_.replace("\n", "") for _ in f.readlines()]
    return len(names), names


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--save_path', type=str, help='coco128.yaml saved path')
    parser.add_argument('--lr0' ,default=0.01, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--lrf' ,default=0.2, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--momentum' ,default=0.937, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--weight_decay' ,default=0.0005, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--warmup_epochs' ,default=3.0, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--warmup_momentum' ,default=0.8, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--warmup_bias_lr' ,default=0.1, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--box' ,default=0.05, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--cls' ,default=0.5, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--cls_pw' ,default=1.0, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--obj' ,default=1.0, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--obj_pw' ,default=1.0, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--iou_t' ,default=0.20, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--anchor_t' ,default=4.0, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--fl_gamma' ,default=0.0, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--hsv_h' ,default=0.015, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--hsv_s' ,default=0.7, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--hsv_v' ,default=0.4, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--degrees' ,default=0.0, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--translate' ,default=0.1, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--scale' ,default=0.5, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--shear' ,default=0.0, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--perspective' ,default=0.0, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--flipud' ,default=0.0, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--fliplr' ,default=0.5, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--mosaic' ,default=1.0, type=str, help='please check hyp_scratch.yaml')
    parser.add_argument('--mixup' ,default=0.0, type=str, help='please check hyp_scratch.yaml')
    
    opt = parser.parse_args()
    opt_dict = vars(opt)

    if opt.save_path:
        # check args
        error_val = ['save_path']
        for key ,value in opt_dict.items():
            if value=='' or value==None or value=="None":
                opt_dict[key]=HYP_SCRATCH_DICT[key]             

        nc , names= load_obj_names()
        write_template(nc=nc, names=names, saved_path=opt.save_path)
        write_hyp_scratch(
            saved_path=opt.save_path, 
            hyp_scratch_dict=opt_dict  
                )
