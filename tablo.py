import tableauserverclient as TSC
import config
import os
import pandas as pd

auth = config.TableauAuth
cred = config.Credentials

#Import of credentials and logins from config file

server = TSC.Server(auth["server"], use_server_version=True)
credentials = TSC.ConnectionCredentials(cred["user"], cred["password"], embed=True)

def sign_in(site_id, option):
    if option == True:
        tableau_auth = TSC.TableauAuth(auth["username"], auth["password"], site_id)
        server.auth.sign_in(tableau_auth)
        #Signs into the site using the tableau authentification information
    else: server.auth.sign_out()

class server_con():
    def __init__(self, site, type, name):
        self.site = site
        self.projs = list(TSC.Pager(server.projects, TSC.RequestOptions(pagenumber=1)))
        self.type = type
        self.name = name

        request_options = TSC.RequestOptions(pagenumber=1)
        if type == "ds":
            self.item_list = list(TSC.Pager(server.datasources, request_options)) #Gets list of datasource items in site
        elif type =="wb":
            self.item_list = list(TSC.Pager(server.workbooks, request_options)) #Gets list of workbook items in site

        if name != None:
            if name == "*":
                self.temp_name = ""
                self.star_name = True
            elif "*" in name:
                self.temp_name = name.split("*")[0]
                self.star_name = True
            else:
                self.temp_name = None
                self.star_name = False

    def get_report(self):
        index = 0
        df = pd.DataFrame(columns=["Workbook name", "Workbook project", "Data source"])
        for item in self.item_list:
            project = item.project_id
            for proj1 in self.projs:
                if proj1.id == project and proj1.parent_id != None:
                    for proj2 in self.projs:
                        if proj1.parent_id == proj2.id:
                            project = proj1.name + "/" + proj2.name
            server.workbooks.populate_connections(item)
            for con in item.connections:
                df.at[index] = [item.name, project, con.datasource_name]
                index += 1
        with pd.ExcelWriter('WBsummary.xlsx') as writer:
            df.to_excel(writer)

    def list(self, proj_id):
        count = 0
        if self.type == "wb": # or self.type == "ds": # Uncomment if no need for unique dss
            for item in self.item_list:
                if proj_id == None:
                    print("Name: " + item.name + ", Project: " + item.project_name)
                    count += 1
                else:
                    if item.project_id == proj_id:
                        print("Name: " + item.name + ", Project: " + item.project_name)
                        count += 1

        elif self.type == "proj":
            for proj in self.projs:
                parent_id = proj.parent_id
                if parent_id != None:
                    for parent_proj in self.projs:
                        if parent_proj.id == parent_id:
                            print("Project: " + proj.name + " Parent: " + parent_proj.name)
                            count += 1
                else:
                    print("Project: " + proj.name)
                    count += 1

        elif self.type == "ds": #Gets and prints list of unique datasources
            ds_list = []
            if proj_id == None:
                for ds in self.item_list:
                    if ds.name not in ds_list:
                        ds_list.append("Name: " + ds.name + ", Project: " + ds.project_name)
            else:
                for ds in self.item_list:
                    if ds.name not in ds_list and ds.project_id == proj_id:
                        ds_list.append("Name: " + ds.name + ", Project: " + ds.project_name)

            for item in ds_list:
                print(item)
            print(len(ds_list))

        if count != 0: print("Count: ", count)

    def con_list(self):
        for item in self.item_list:
            # print("Workbook " + item.name + " connections:")
            project = item.project_id
            for proj1 in self.projs:
                if proj1.id == project and proj1.parent_id != None:
                    for proj2 in self.projs:
                        if proj1.parent_id == proj2.id:
                            project = proj1.name + "/" + proj2.name
                            print(project)
            if self.type == "wb":
                server.workbooks.populate_connections(item)
                for con in item.connections:
                    print("Project path: " + project +", Datasource name: " +
                          con.datasource_name + ", Datasource id: " + con.datasource_id)
            elif self.type == "ds":
                server.datasources.populate_connections(item)
                for con in item.connections:
                    print("Datasource project: " + project + ", Connection type: " +
                          con.connection_type + ", Connection ID: " + con.id)


    def finder(self, proj_id, dsite, dproj_id, cmd, extract):
        found = False
        if proj_id != None:
            if self.temp_name != None:
                self.name = self.temp_name
            for item in self.item_list:
                if ((not self.star_name and self.name == item.name) or (self.star_name
                                                                        and self.name == item.name[:len(self.name)]))\
                                                                        and item.project_id == proj_id:
                    path = self.downloader(item, cmd, dsite, dproj_id, extract)
                    found = True
                    if cmd == "change_filter":
                        return path
                if not found and item == self.item_list[len(self.item_list)-1]:
                    print("Item was not found, please input a different name")
                    exit()

        else:
            for item in self.item_list:
                if (not self.star_name  and  self.name == item.name) or\
                        (self.star_name and self.temp_name == item.name[:len(self.temp_name)]):
                    path = self.downloader(item, cmd, dsite, dproj_id, extract)
                    found = True
                    if cmd == "change_filter":
                        return path
                if not found and item == self.item_list[len(self.item_list)-1]:
                    print("Item was not found, please input a different name")
                    exit()
        print("All tasks completed")

    def downloader(self, item, cmd, dsite, dproj_id, extract):
        print("Downloading...")
        if self.type=="wb":
            file_path = server.workbooks.download(item.id, no_extract=extract)
        elif self.type=="ds":
            file_path = server.datasources.download(item.id, no_extract=extract)
        else:
            print("Type unknown, please input type wb or ds for downloads")
            exit()
        print("Downloaded")
        if cmd == "copy":
            self.publisher(item, dsite, dproj_id, file_path)
        else:
            return file_path


    def publisher(self, item, dsite, dproj_id, path):
        print("Publishing...")
        self.alt_site(dsite, "in")
        if self.type == "ds":
            new_ds = TSC.DatasourceItem(dproj_id, item.name)
            try:
                server.datasources.publish(new_ds, file_path=path, mode="Overwrite", connection_credentials=credentials)
            except Exception as e:
                print("There was an error publishing " + item.name, "Exception: " + str(e))
        elif self.type == "wb":
            new_wb = TSC.WorkbookItem(dproj_id, item.name)
            try:
                server.workbooks.publish(new_wb, file_path=path, mode="Overwrite", connection_credentials=credentials)
            except Exception as e:
                print("There was an error publishing " + item.name, "Exception: " + str(e))
        self.alt_site(dsite, "out")
        os.remove(path)
        print("Published")

    def project_finder(self, proj_path, site_projs):
        index = len(proj_path) - 1
        project = proj_path[index] #Takes last item in list (lowest directory)
        for proj in site_projs:
            if project == proj.name: #Finds project with same name in site
                if len(proj_path) < 2 and proj.parent_id == None:
                    #Checks if there is no parent project and whether the found project has the same name and no parent
                    return proj
                elif self.parent_checker(proj.parent_id, proj_path[index-1], site_projs):
                    try:
                        self.project_finder(proj_path[:index-1], site_projs)
                        #Removes the lowest dir and repeats the process until there are less than two elements
                    except:
                        return proj

    def parent_checker(self, parent_id, parent_name, site_projs):
        #Checks whether the parent id of the lowest project matches the id and name of the parent directory
        for proj in site_projs:
            if proj.id == parent_id and parent_name == proj.name:
                return True
        return False

    def alt_site(self, dsite, in_out):
        if dsite != None:
            if in_out == "in":
                sign_in(dsite, True)
            elif in_out == "out":
                sign_in(None, False)
                sign_in(self.site, True)