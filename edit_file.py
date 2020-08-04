from zipfile import ZipFile as zip
import os

def unzip_tds(file_path):
    with zip(file_path, 'r') as f:
        file = [file for file in f.namelist() if file.endswith(".tds")].pop()
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



