import tablo
import argparse
import edit_file
import tableauserverclient as TSC
import json
import os

#TODO:
'''
- Test for deficiencies and bugs;
- Fix formulas of measures, because now it's strange xml;
- Find a way to extract measures from other sites;
- Try and understand why some workbooks refuse to be published to other projects and or websites.
'''
parser = argparse.ArgumentParser(description='Executes tasks on workbooks')
parser.add_argument('site', help='Site id from which workbooks will be copied or listed')
parser.add_argument('cmd', help='Command for program, ex: copy, list, download, change_filter, get_report')
parser.add_argument("type", help='Object type ex: ds, wb, proj')
parser.add_argument('-proj', '--project', help='Project path from which to copy or list, True for project list')
parser.add_argument('--name', help='Name of obj to copy or whose connections to list')
parser.add_argument('-c', '--connections', type=bool, default=False, help='Boolean of whether you want connections listed')
parser.add_argument('-dsite', '--destination_site', help='Destination site for copying')
parser.add_argument('-dproj', '--destination_project', help='Destination project for copying')
parser.add_argument('-fl', '--filter_list', help='List of filters to add to data source separated by commas')
parser.add_argument('-ne','--no_extract', type=bool, default=False, help='Data source with or without extract, default False')

args = parser.parse_args()

site = args.site
cmd = args.cmd
type = args.type
proj = args.project
name = args.name
cons = args.connections
dsite = args.destination_site
dproj = args.destination_project
fl = args.filter_list
extract = args.no_extract
dprojs = None

if site == "Default":
    site = None

if cmd == "copy" and ((name == None) or (dproj == None)):
    parser.error('Copying requires a name and destination project')
elif cmd == "change_filter" and (type != "ds" or name == None or fl == None):
    parser.error('Changing filters requires a ds name and a list of filters')

tablo.sign_in(site, True)
tsc = tablo.server_con(site, type, name)

def path_splitter(new_proj):
    if new_proj != None:
        if dsite == None or new_proj == proj:
            new_proj = new_proj.split("/") #Splits the path into a list of directories
            project = tsc.project_finder(new_proj, tsc.projs) #Gets the desired project
        else:
            tablo.sign_in(dsite, True) #Signs into destination site
            new_proj = new_proj.split("/")
            request_options = TSC.RequestOptions(pagenumber=1)
            dprojs = list(TSC.Pager(tablo.server.projects, request_options))#Gets all projects in destination site
            project = tsc.project_finder(new_proj, dprojs)
            tablo.sign_in(None, False) #Signs out of destination site
            tablo.sign_in(site, True) #Signs back into the main site
        return project

def commander():
    try:
        proj1 = proj.id
    except:
        proj1 = proj

    if cmd == "list":
        if cons:
            tsc.con_list()
        else:
            tsc.list(proj1)

    elif cmd == "copy" or cmd == "download":
        tsc.finder(proj1, dsite, dproj, cmd, extract)

    elif cmd == "change_filter":
        tdsx_path = tsc.finder(name, proj1, dsite, dproj, cmd)
        filter_list = fl.split(",")
        tds_file = edit_file.unzip(tdsx_path)
        edit_file.filter_change(tds_file, tdsx_path, filter_list)
        tsc.publisher(tdsx_path, dsite, dproj, tdsx_path)

    elif cmd == "get_report":
        tsc.get_report()

    elif cmd == "update_measures":
        dict = {}
        for wb in tsc.item_list:
            path_zip = tsc.downloader(wb, cmd, dsite, dproj.id, extract)
            if ".twbx" in path_zip:
                path = edit_file.unzip(path_zip).split(".")[0]
            else:
                path = wb.name
            dict = edit_file.write_measures(path, dict)
            os.remove(path_zip)

        with open('MeasuresFile.json', 'w') as outfile:
            json.dump(dict, outfile, indent=2)

    elif cmd == "get_measures":
        if name == None:
            print("Please provide the name of the measure.")
            exit()
        else:
            with open('MeasuresFile.json') as json_file:
                data = json.load(json_file)
                for measure in data:
                    if name in measure:
                        print("Name: " + data[measure]["name"])
                        print("ID: " + data[measure]["id"])
                        print("Workbook name: " + data[measure]["wb_name"])
                        print("Formula: " + data[measure]["formula"])

    else:
        print("Wrong command provided")
        exit()

if __name__ == "__main__":
    proj = path_splitter(proj)
    dproj = path_splitter(dproj)
    commander()
