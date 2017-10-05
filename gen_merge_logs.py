import lasio
import pandas as pd
import numpy as np


root = r'C:\Users\joanna.wallis\Documents\FORCE_presentation\FORCE_catcher\FRM'
wells = ["21_24-1", "21_24-4", "21_24-5", "21_24-6", "21_24-7", "21_25-10", "21_25-8","21_25-9"]#["21_24-1"] ["21_24-5", "21_24-7"] #
scenarios = ["_100WTR","_05OIL", "_70OIL", "_95OIL", "_05GAS", "_70GAS", "_95GAS"]#[""]
#scenarios = [""]
#wd = [289, 282]
wd = [279, 292, 289, 280, 282, 299, 301, 299]


#merge_root = r'C:\Users\joanna.wallis\Documents\FORCE_presentation\FORCE_catcher\Merge\TVDSS'
merge_root = r'C:\Users\joanna.wallis\Documents\FORCE_presentation\FORCE_catcher\dev'
#merge_suffix = "_logs"
merge_suffix = "_md2tvd"
merge_depth_col = "Md ft"

out_root = r'C:\Users\joanna.wallis\Documents\FORCE_presentation\FORCE_catcher\FRM\TVD'

depth_column = "Md"
null = -999.25

datum_dict = {}
for well, depth in zip(wells, wd):
    datum_dict[well] = depth

filepaths = []

# load main las

for well in wells:

    # load merge file

    logs_dict = {}
    merge_path = merge_root + "\\" + well + merge_suffix + ".las"


    data = lasio.read(merge_path)
    logs_merge = []
    units_merge = []
    for curve in data.curves:
        logs_merge.append(curve.mnemonic)
        units_merge.append(curve.unit)
        logs_dict[curve.mnemonic] = curve.unit

    #print (logs_merge)
    merge_df = pd.DataFrame()

    for log in logs_merge:
        merge_df[str(log)] = data[str(log)]
        merge_df[str(log)] = np.where(merge_df[str(log)] == null, np.nan, merge_df[str(log)])



    merge_df.rename(columns = {merge_depth_col: "Md"}, inplace=True)
    #print (merge_df)
    #merge_df["TVDML"] = merge_df["TVDSS"] - datum_dict.get(well)
    #logs_dict["TVDSS"] = logs_dict.pop("UNKNOWN")
    #logs_dict["TVDML"] = "ft"


    #print(merge_df)

    # load main las file
    for scenario in scenarios:
        path = root + "\\" + well + scenario + ".las"
        filepaths.append(path)

        data_main = lasio.read(path)
        logs_main = []
        units_main = []
        for curve in data_main.curves:
            logs_main.append(curve.mnemonic)
            units_main.append(curve.unit)
            logs_dict[curve.mnemonic] = curve.unit

        main_df = pd.DataFrame()

        for log in logs_main:
            main_df[str(log)] = data_main[str(log)]
            main_df[str(log)] = np.where(main_df[str(log)] == null, np.nan, main_df[str(log)])

        print(main_df)
        output = pd.merge(main_df, merge_df, on = depth_column, how = "outer")
        #print (output)
        output_file = out_root + "\\" + well + scenario + ".las"

        output.dropna(how = "all", axis = 0, inplace = True)
        output.sort_values(by = "Md", inplace = True)

        with open(output_file, mode = 'w') as lasfile:
            las = lasio.LASFile()
            las.depth = [depth_column]
            las.well["WELL"].value = str(well)
            las.well["NULL"].value = null
            for log in list(output.columns.values):
                las.add_curve(log, output[log], unit = logs_dict.get(log))
            las.write(lasfile, version=2, fmt = "%10.9g")








