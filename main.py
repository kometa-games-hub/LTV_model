import pandas as pd
import numpy as np
from app import client
from scipy import optimize
import ltv_sql


configs = {
    'media_source': ['Organic', 'Facebook Ads', 'googleadwords_int', 'unityads_int', 'Apple Search Ads'],
    'Tier-1': [
        'United States', 'France', 'Germany', 'United Kindgom', 'Canada', 'Australia', 'Italy', 'Australia', 'Netherlands',
        'Norway', 'Belgium', 'Puerto Rico', 'Slovenia', 'Sweden', 'Spain', 'Denmark', 'Turkey', 'France', 'Romania'
               ]
}

platform = "'android'"
utm_source = "'Facebook Ads'"
year = 2022
month_start = '09'
month_end = '09'
day_off = 30

# '2022-01-01' and '2022-01-31'

date_string = f"'{year}-{month_start}-01' and '{year}-{month_end}-{day_off}'"

#print(date_string)

players_type = 'payers'

def compute_ltv_coeffs():
    query = ltv_sql.ltv_query.format(**globals())
    #print(query)
    query_payers = ltv_sql.ltv_payers_query.format(**globals())
    #print(query_payers)
    ltv_data = client.query_dataframe(query_payers)

    #print(ltv_data.to_string())

    if not ltv_data.empty:
        def ltv_func(param, coefs):
            result = coefs[0] + coefs[1] * np.log(param)
            return result

        if players_type == 'payers':
            ltv_data['players'] = ltv_data['players'].cumsum()
        ltv_data['cumulative_revenue'] = ltv_data['revenue'].cumsum()


        ltv_data['ltv'] = ltv_data['cumulative_revenue'] / ltv_data['players']

        #print(ltv_data.to_string())

        df_total = pd.DataFrame(columns=['forecast_day', 'ltv_forecast', 'coeff', 'ltv'])
        for past_day in (3, 7, 14, 30):
            #past_day = 8
            # Прогноз в прошлое
            real_X_L = [i + 1 for i in range(past_day)]
            real_Y_L = ltv_data['ltv'].values[:past_day]
            real_days = np.hstack([real_X_L, [i for i in range(past_day, ltv_data['day'].max())]])
            real_coefs_l, real_cov = optimize.curve_fit(lambda t, a, b: a + b * np.log(t), real_X_L, real_Y_L)
            ltv_real_forecast = ltv_func(real_days, real_coefs_l)

            ltv_past_forecast_data = pd.DataFrame(columns=['day', 'ltv_forecast'])
            ltv_past_forecast_data['day'] = real_days
            ltv_past_forecast_data['ltv_forecast'] = ltv_real_forecast

            past_ltv_data = ltv_data[ltv_data['day'] > past_day]
            ltv_real_forecast_data = ltv_past_forecast_data[ltv_past_forecast_data['day'] > past_day]

            # if past_ltv_data.ltv.max() > ltv_real_forecast_data.ltv_forecast.min():
            # ltv_real_forecast_data.ltv_forecast = ltv_real_forecast_data.ltv_forecast + \ (past_ltv_data.ltv.max()
            # - ltv_real_forecast_data.ltv_forecast.min())

            final_ltv_data = pd.merge(past_ltv_data, ltv_real_forecast_data, how='inner', on='day')
            # print('ltv')
            # print(final_ltv_data[final_ltv_data.day == 100][['day', 'ltv_forecast', 'ltv']].to_string())


            df_total = df_total.append({'forecast_day': past_day,
                                        'ltv_forecast': list(final_ltv_data[final_ltv_data.day == 100]['ltv_forecast'])[0],
                                        'ltv': list(final_ltv_data[final_ltv_data.day == 100]['ltv'])[0]
                                        }, ignore_index=True)

            final_ltv_data['coeff'] = final_ltv_data.ltv / final_ltv_data.ltv_forecast
            final_ltv_data = final_ltv_data.pivot_table(index=final_ltv_data.index, columns=['day'], values='coeff')
            final_ltv_data = final_ltv_data.apply(lambda x: pd.Series(x.dropna().values))

            df_total.loc[df_total['forecast_day'] == past_day, ['coeff']] = list(final_ltv_data[100])[0]

            # final_ltv_data.insert(loc=0, column='country_tier', value='Tier-1')
            # final_ltv_data.insert(loc=0, column='media_source', value='Organic')
            # final_ltv_data.insert(loc=0, column='month', value=month)
            #final_ltv_data.insert(loc=0, column='year', value=year)
            final_ltv_data.insert(loc=0, column='forecast_day', value=past_day)

            final_ltv_data = final_ltv_data[['forecast_day', 100]].copy()

            #df_total = df_total.astype(float).fillna(0).round(4)



        df_total = df_total.astype(float).fillna(0).round(4).astype(str)
        df_total = df_total.stack().str.replace('.', ',').unstack()
        df_total['forecast_day'] = df_total['forecast_day'].str.replace(',0', '')
        df_total = df_total[df_total.columns].astype(str).apply(lambda x: '; '.join(x), axis=1)
        print(df_total.to_string(header=None, index=None))
    else:
        ltv_forecast_data = pd.DataFrame()
        ltv_real_forecast_data = pd.DataFrame()


if __name__ == '__main__':
    compute_ltv_coeffs()
