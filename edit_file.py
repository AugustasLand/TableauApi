from zipfile import ZipFile as zip
import os
import json

def unzip(file_path):
    with zip(file_path, 'r') as f:
        file = [file for file in f.namelist() if file.endswith(".tds") or file.endswith(".twb")].pop()
        f.extract(file)
    return file

def filter_change(tds_file, tdsx_path, filter_list):
    with open(tds_file, "r") as t:
        temp_file = t.read().splitlines()
        x = 0
        index = temp_file.index([line for line in temp_file if "filter class='categorical'" in line].pop())
        line = temp_file[index]
        while "/filter" not in line:
            if "member=" in line:
                if x == len(filter_list):
                    print(line)
                    temp_file.remove(line)
                    index -= 1
                    print(line)
                else:
                    filter_name = line.split("member=")[1].split("&quot;")[1]
                    temp_file[index] = line.replace(filter_name, filter_list[x])
                    x += 1
                if "/groupfilter" in temp_file[index + 1] and x != len(filter_list):
                    new_line = line.replace(filter_name, filter_list[x])
                    temp_file.insert(index, new_line)
                    x += 1
            index += 1
            line = temp_file[index]

    os.remove(tds_file)
    with open(tds_file, 'x') as nf:
        for line in temp_file:
            nf.write(line + "\n")

    with zip(tdsx_path, 'w') as z:
        z.write(tds_file)

def write_measures(wb_name, dict):
    with open(wb_name + ".twb", "r") as t:
        temp_file = t.read().splitlines()
        pre_line = ""
        for line in temp_file:
            if "formula" in line and "caption" in pre_line:
                formula = line.split("formula='")[1].split("'")[0]
                formula = formula.replace("&", "").replace("#", "").replace("\\", "")
                try:
                    name = pre_line.split("caption='")[1].split("'")[0]
                    id = pre_line.split("name='")[1].split("'")[0]
                    update = True
                    for item in dict:
                        if id == dict[item]["id"]:
                            update = False
                            break
                    if update:
                        dict.update({name: {"id": id, "formula": formula, "wb_name": wb_name}})
                except:
                    pass
            pre_line = line
    return dict

