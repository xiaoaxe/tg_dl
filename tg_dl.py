
import json
import sys
import random

input_file = "/Users/baoqiang/Downloads/tg/result.json"
# input_file = "/Users/baoqiang/Downloads/tg/sample.json"
output_path = "/Users/baoqiang/Downloads/tg/results"

# python3 -u "/Users/baoqiang/Downloads/tg/tg_dl.py"
def split_tg():
    idx = 1
    step = 100

    with open(input_file, 'r') as f:
        jdata = json.loads(f.read())
        ndata = {}

        for k, v in jdata.items():
            if k != 'messages':
                ndata[k] = v

        # 打乱顺序
        random.shuffle(jdata['messages'])
        messages = []
        num = 0
        for msg in jdata['messages']:
            # 过滤类型
            if msg.get('media_type') != 'video_file':
                # print(msg)
                continue
            
            #过滤时长
            if msg.get('duration_seconds', 100) > 60:
                continue

            if num < step:
                messages.append(msg)
                num += 1
            else:
                # 赋值工作
                ndata["messages"] = messages

                output_file = '{}/result_{:0>3d}.json'.format(output_path, idx)
                with open(output_file, 'w') as fw:
                    fw.write(f"{json.dumps(ndata, indent=4, ensure_ascii=False)}\n")
        
                #清零工作
                messages = []
                ndata["messages"] = []
                num = 0

                #下一步
                print(f"process idx: {idx}")
                idx += 1
                sys.stdout.flush()

        if len(messages) > 0:
            ndata["messages"] = messages
            output_file = '{}/result_{:0>3d}.json'.format(output_path, idx)
            with open(output_file, 'w') as fw:
                fw.write(f"{json.dumps(ndata, indent=4, ensure_ascii=False)}\n")
            print(f"process idx: {idx}")

if __name__ == "__main__":
    split_tg()
