# -*- coding: utf-8 -*-
# @Time    : 2023/12/11 16:37
# @Author  : XieSJ
# @FileName: main.py
from collections import defaultdict

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def group_func(data, column):
    data = data.dropna(subset=['Area', 'Year', 'Value', column])
    group_data = data[['Area', 'Year', 'Value', column]].groupby(by=['Area', 'Year', column]).mean().to_dict()
    res_dict = defaultdict(dict)
    for k, v in group_data['Value'].items():
        country, date, element = k
        res_dict[element][(country, date)] = v
    return res_dict


def four_weather_graph(weather):
    weather_dropna = weather.dropna(subset=['country_name', 'TAVG', 'TMAX', 'TMIN', 'PRCP'])
    weather_data = weather_dropna[['country_name', 'TAVG', 'TMAX', 'TMIN', 'PRCP', 'DATE']]
    weather_group_data = weather_data[['country_name', 'TAVG', 'TMAX', 'TMIN', 'PRCP', 'DATE']].groupby(
        by=['country_name', 'DATE']).mean().to_dict()
    fbs = pd.read_csv("data/FBS_data.csv", encoding='gbk')
    gt = pd.read_csv("data/GT_data.csv", encoding='gbk')
    qcl = pd.read_csv("data/QCL_data1.csv", encoding='gbk')

    fbs_data = group_func(fbs, 'Element')
    gt_data = group_func(gt, 'Element')
    qcl_data = group_func(qcl, 'Item')
    qcl_data = {k: v for k, v in qcl_data.items() if k == "Apples"}
    merged_dict = {key: value for d in [fbs_data, gt_data, qcl_data] for key, value in d.items()}

    tavg = weather_group_data['TAVG']
    TMAX = weather_group_data['TMAX']
    TMIN = weather_group_data['TMIN']
    PRCP = weather_group_data['PRCP']
    tavg_keys = set(tavg.keys())
    TMAX_keys = set(TMAX.keys())
    TMIN_keys = set(TMIN.keys())
    PRCP_keys = set(PRCP.keys())
    # 合并
    columns = ['TAVG', 'TMAX', 'TMIN', 'PRCP']
    merged_values = [set(v.keys()) for k, v in merged_dict.items()]
    join_key = tavg_keys & TMAX_keys & TMIN_keys & PRCP_keys
    for merged_value in merged_values:
        join_key = join_key & merged_value
    columns.extend(list(fbs_data.keys()))
    columns.extend(list(gt_data.keys()))
    columns.extend(list(qcl_data.keys()))
    no_weather_columns = list(fbs_data.keys()) + list(gt_data.keys()) + list(qcl_data.keys())
    all_df = pd.DataFrame(columns=columns)
    for k, tavg_datum in tavg.items():
        if k in join_key:
            print()
        if TMAX.get(k) and TMIN.get(k) and PRCP.get(k) and all([i.get(k) for i in merged_dict.values()]):
            datum = [tavg_datum, TMAX.get(k), TMIN.get(k), PRCP.get(k)]
            for no_weather_column in no_weather_columns:
                datum.append(merged_dict[no_weather_column][k])
            all_df.loc[len(all_df)] = datum
    # 热力图
    plt.figure(figsize=(15, 12))  # 设置图形大小
    correlation_matrix = all_df.corr()

    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')  # annot参数用于显示数值，cmap参数指定颜色映射，fmt参数指定显示的数值格式
    plt.title('Heatmap of DataFrame')  # 设置标题
    plt.show()


if __name__ == '__main__':
    weather = pd.read_csv("data/cleaned_yearly_weather_data.csv", encoding='gbk')
    weather = weather.dropna(subset=['NAME'])
    weather['country'] = [i.split(',')[-1].strip() for i in list(weather['NAME'])]
    with open("data/country_codes.txt", 'r', encoding='utf-8') as f:
        country_codes = f.readlines()
    country_list = [i.strip().split('\t') for i in country_codes[1:]]
    country_dict = {i[1]: i[0] for i in country_list}
    countries = list(weather['country'])
    country_names = []
    for country in countries:
        if country_dict.get(country):
            country_names.append(country_dict[country])
        else:
            country_part_dict = {k: v for k, v in country_dict.items() if country in k or k in country}
            if len(country_part_dict) == 1:
                country_names.append(list(country_part_dict.values())[0])
            else:
                country_names.append(None)
    weather['country_name'] = country_names
    weather['DATE'] = weather['DATE'].astype('int')
    four_weather_graph(weather)
    # 四个天气
    weather_columns = ['TAVG', 'TMAX', 'TMIN', 'PRCP']
    for weather_column in weather_columns:
        weather_dropna = weather.dropna(subset=['country_name', weather_column])
        weather_data = weather_dropna[['country_name', weather_column, 'DATE']]
        weather_group_data = weather_data[['country_name', weather_column, 'DATE']].groupby(
            by=['country_name', 'DATE']).mean().to_dict()
        fbs = pd.read_csv("data/FBS_data.csv", encoding='gbk')
        gt = pd.read_csv("data/GT_data.csv", encoding='gbk')
        qcl = pd.read_csv("data/QCL_data1.csv", encoding='gbk')

        fbs_data = group_func(fbs, 'Element')
        gt_data = group_func(gt, 'Element')
        qcl_data = group_func(qcl, 'Item')
        qcl_data = {k: v for k, v in qcl_data.items() if k == "Apples"}
        merged_dict = {key: value for d in [fbs_data, gt_data, qcl_data] for key, value in d.items()}

        weather_column_data = weather_group_data[weather_column]
        # 合并
        columns = [weather_column]
        columns.extend(list(fbs_data.keys()))
        columns.extend(list(gt_data.keys()))
        columns.extend(list(qcl_data.keys()))
        no_weather_columns = list(fbs_data.keys()) + list(gt_data.keys()) + list(qcl_data.keys())
        all_df = pd.DataFrame(columns=columns)
        for k, weather_datum in weather_column_data.items():
            if all([i.get(k) for i in merged_dict.values()]):
                datum = [weather_datum]
                for no_weather_column in no_weather_columns:
                    datum.append(merged_dict[no_weather_column][k])
                all_df.loc[len(all_df)] = datum
        # 热力图
        plt.figure(figsize=(15, 12))  # 设置图形大小
        correlation_matrix = all_df.corr()

        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')  # annot参数用于显示数值，cmap参数指定颜色映射，fmt参数指定显示的数值格式
        plt.title(f'Heatmap of DataFrame {weather_column}')  # 设置标题
        plt.show()
