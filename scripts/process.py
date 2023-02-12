from dataflows import Flow, load, dump_to_path, add_metadata, printer, update_resource, unpivot
from dataflows.helpers import ResourceMatcher
import csv 
import openpyxl
import os

def rename_column(from_name, to_name, resources=None):
    def renamer(rows):
        for row in rows:
            yield dict(
                (k if k != from_name else to_name, v) for k, v in row.items()
            )

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                for field in resource.get("schema", {}).get("fields", []):
                    if field["name"] == from_name:
                        field["name"] = to_name
        yield package.pkg
        for res in package:
            if matcher.match(res.res.name):
                yield renamer(res)
            else:
                yield res

    return func

def xlsx_to_csv():
    inputExcelFile = 'archive/Water_Basins_KZ.xlsx'
    wb = openpyxl.load_workbook(inputExcelFile)
    ws_water_basins_main = wb["Basins_KZ"]
    ws_water_basins_lakes = wb["Lakes_KZ"]
    ws_water_basins_rivers = wb["Rivers_KZ"]
    ws_water_basins_water_consumption = wb["Water_consumption"]
    OutputCsvFile1 = csv.writer(open("data/water_basins_kz_v1.csv", 'w'), delimiter=",")
    OutputCsvFile2 = csv.writer(open("data/water_basins_lakes_v1.csv", 'w'), delimiter=",")
    OutputCsvFile3 = csv.writer(open("data/water_basins_rivers_v1.csv", 'w'), delimiter=",")
    OutputCsvFile4 = csv.writer(open("data/water_basins_water_comsumption_v1.csv", 'w'), delimiter=",")
    for eachrow in ws_water_basins_main.rows:
        OutputCsvFile1.writerow([cell.value for cell in eachrow])
    for eachrow in ws_water_basins_lakes.rows:
        OutputCsvFile2.writerow([cell.value for cell in eachrow])
    for eachrow in ws_water_basins_rivers.rows:
        OutputCsvFile3.writerow([cell.value for cell in eachrow])
    for eachrow in ws_water_basins_water_consumption.rows:
        OutputCsvFile4.writerow([cell.value for cell in eachrow])
def clean_water_basins_kz():
    with open("data/water_basins_kz_v1.csv","r", newline="") as fin, open("data/water_basins_kz_v2.csv","w") as fout:
        writer=csv.writer(fout)
        for row in csv.reader(fin):
            if any(row):
                writer.writerow(row[1:-1])
def clean_water_basins_lakes_kz():
    with open("data/water_basins_lakes_v1.csv","r", newline="") as fin, open("data/water_basins_lakes_v2.csv","w") as fout:
        writer=csv.writer(fout)
        for row in csv.reader(fin):
            if any(row):
                writer.writerow(row[4:])

def clean_water_basins_rivers_kz():
    with open("data/water_basins_rivers_v1.csv","r", newline="") as fin, open("data/water_basins_rivers_v2.csv","w") as fout:
        writer=csv.writer(fout)
        for row in csv.reader(fin):
            if any(row):
                writer.writerow(row[0:3])

def clean_water_basins_water_comsumption_kz():
    with open("data/water_basins_water_comsumption_v1.csv","r", newline="") as fin, open("data/water_basins_water_comsumption_v2.csv","w") as fout:
        writer=csv.writer(fout)
        for row in csv.reader(fin):
            if any(row):
                writer.writerow(row[2:8])

def water_resources_and_demographics_process():
    xlsx_to_csv()
    clean_water_basins_kz()
    clean_water_basins_lakes_kz()
    clean_water_basins_rivers_kz()
    flow = Flow(
        load("data/water_basins_kz_v2.csv", format='csv', name='water-basins-kz'),
        rename_column("Basins_KZ", "basins_kz", "water-basins-kz"),
        rename_column("Square(sq)", "square", "water-basins-kz"),
        rename_column("Water_resources_KZ(cubicmeter)", "water_resources_kz", "water-basins-kz"),
        rename_column("Regions_KZ", "regions_kz", "water-basins-kz"),
        rename_column("Basins_Population", "basins_population", "water-basins-kz"),
        rename_column("Urban_Basins_Population", "urban_basins_population", "water-basins-kz"),
        rename_column("Rural_Basins_Population", "rural_basins_population", "water-basins-kz"),
        rename_column("Rivers_of_Basins", "rivers", "water-basins-kz"),
        rename_column("River_length_in_KZ()", "river_length_in_kz", "water-basins-kz"),
        update_resource('water-basins-kz', path='data/water-basins-kz'),
        
        load("data/water_basins_lakes_v2.csv", format='csv', name='water-basins-lakes'),
        rename_column("Lakes_KZ", "lakes_kz", "water-basins-lakes"),
        rename_column("Square,Â²", "square", "water-basins-lakes"),
        rename_column("Regions", "regions", "water-basins-lakes"),
        update_resource('water-basins-lakes', path='data/water-basins-lakes'),
        
        load("data/water_basins_rivers_v2.csv", format='csv', name='water-basins-rivers'),
        rename_column("Rivers_KZ", "rivers_kz", "water-basins-rivers"),
        rename_column("River_Length", "river_length", "water-basins-rivers"),
        rename_column("River_Length_in_KZ", "river_length_in_kz", "water-basins-rivers"),
        update_resource('water-basins-rivers', path='data/water-basins-rivers'),
        
        load("data/water_basins_water_comsumption_v2.csv", format='csv', name='water-basins-water-comsumption'),
        rename_column("Rivers_KZ", "rivers_kz", "water-basins-water-comsumption"),
        rename_column("Rivers_length,", "rivers_length", "water-basins-water-comsumption"),
        rename_column("River_fall,m", "rivers_fall_m", "water-basins-water-comsumption"),
        rename_column("Average_annual_water_consumption,m3/s", "avg_annual_water_consumption_m3_s", "water-basins-water-comsumption"),
        rename_column("Water_and_energy_resources,Power,thousandkW", "water_and_energy_resources_power_thousand_kw", "water-basins-water-comsumption"),
        rename_column("Waterandenergyresources,Energy,millionkWh/year", "water_and_energy_resources_power_mln_kwh_year", "water-basins-water-comsumption"),        
        update_resource('water-basins-water-comsumption', path='data/water-basins-water-comsumption'),
        
        
        
        add_metadata(name='water-resources-and-demographics-kz', title='''Water resources and demographics'''),
        dump_to_path(),
    )
    flow.process()
    os.remove("data/water_basins_kz_v1.csv")
    os.remove("data/water_basins_kz_v2.csv")
    os.remove("data/water_basins_lakes_v1.csv")
    os.remove("data/water_basins_lakes_v2.csv")
    os.remove("data/water_basins_rivers_v1.csv")
    os.remove("data/water_basins_rivers_v2.csv")
    os.remove("data/water_basins_water_comsumption_v1.csv")
    os.remove("data/water_basins_water_comsumption_v2.csv")
if __name__ == '__main__':
    water_resources_and_demographics_process()
    #clean_water_basins_water_comsumption_kz()