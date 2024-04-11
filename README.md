with all_ad_result as (
select 
  ad_date,
  url_parameters,	
  coalesce(spend,0) as spend,
  coalesce(impressions,0) as impressions,
  coalesce(reach,0) as reach,
  coalesce(clicks,0) as clicks,
  coalesce(leads,0) as leads,
  coalesce(value,0) as value,
  'facebook_ads' as media_source,
  date_trunc('month',ad_date)::date as ad_month, 
        lower(substring(decode_url_part(url_parameters),'utm_campaign=([^&#$]+)')) as utm_campaign,
        case when lower(substring(url_parameters,'utm_campaign=([^&#$]+)'))= 'nan' then null end,
        sum(spend) as sum_spend,
		sum(impressions) as sum_impressions,
		sum(clicks) as sum_clicks,
		sum (value)as sum_value,
		case when sum(clicks)>0 then round(sum(spend)/sum(clicks),4) else NULL end as cpc,
		case when sum(impressions)>0 then round(sum(spend)*1000/sum(impressions),4) else NULL end as cpm,
		case when sum(impressions)>0 then round(sum(clicks)::numeric/sum(impressions),4) else NULL end as ctr,
		case when sum (spend)>0 then round(sum(value)::numeric/sum(spend),4) else NULL end as romi,
  facebook_campaign.campaign_name,
  facebook_adset.adset_name
from facebook_ads_basic_daily fabd
left join facebook_campaign on facebook_campaign.campaign_id = fabd.campaign_id
left join facebook_adset on facebook_adset.adset_id = fabd.adset_id
group by ad_date, url_parameters, media_source, campaign_name, adset_name, fabd.spend,fabd.impressions, fabd.reach, fabd.clicks, fabd.leads,fabd.value
UNION
select
  ad_date,
  url_parameters,
  coalesce(spend,0) as spend,
  coalesce(impressions,0) as impressions,
  coalesce(reach,0) as reach,
  coalesce(clicks,0) as clicks,
  coalesce(leads,0) as leads,
  coalesce(value,0) as value,
  'google_ads' as media_source,
  date_trunc('month',ad_date)::date as ad_month, 
  lower(substring(decode_url_part(url_parameters),'utm_campaign=([^&#$]+)')) as utm_campaign,
  case when lower(substring(url_parameters,'utm_campaign=([^&#$]+)'))= 'nan' then null end,
  sum(spend) as sum_spend,
		sum(impressions) as sum_impressions,
		sum(clicks) as sum_clicks,
		sum (value)as sum_value,
		case when sum(clicks)>0 then round(sum(spend)/sum(clicks),4) else NULL end as cpc,
		case when sum(impressions)>0 then round(sum(spend)*1000/sum(impressions),4) else NULL end as cpm,
		case when sum(impressions)>0 then round(sum(clicks)::numeric/sum(impressions),4) else NULL end as ctr,
		case when sum (spend)>0 then round(sum(value)::numeric/sum(spend),4) else NULL end as romi,
        campaign_name,
        adset_name
from google_ads_basic_daily gabd
group by ad_date, url_parameters, media_source, campaign_name, adset_name,gabd.spend, gabd.impressions,gabd.reach, gabd.clicks, gabd.clicks, gabd.leads, gabd.value
)
select all_ad_result.ad_month,
       all_ad_result.campaign_name,
       all_ad_result.adset_name,
	   all_ad_result.media_source,
	   all_ad_result.utm_campaign,
	   all_ad_result.sum_spend,
	   all_ad_result.sum_impressions,
	   all_ad_result.sum_clicks,
	   all_ad_result.sum_value,
	   all_ad_result.cpc,
	   all_ad_result.cpm,
	   all_ad_result.ctr,
	   all_ad_result.romi,
	   lag (all_ad_result.cpc,1) over (partition by all_ad_result.campaign_name order by all_ad_result.cpc) as prev_cpc,
	   lag (all_ad_result.cpm,1) over (partition by all_ad_result.campaign_name order by all_ad_result.cpm) as prev_cpm,
	   lag (all_ad_result.ctr,1) over (partition by all_ad_result.campaign_name order by all_ad_result.ctr) as prev_ctr,
	   lag (all_ad_result.romi,1) over (partition by all_ad_result.campaign_name order by all_ad_result.romi) as prev_sum_romi,
	   case when lag (all_ad_result.cpc,1) over (partition by all_ad_result.campaign_name order by all_ad_result.cpc)!=0 then round((all_ad_result.cpc / lag (all_ad_result.cpc,1) over (partition by all_ad_result.campaign_name order by all_ad_result.cpc))*100.0,4) end as cpc_diff,
	   case when lag (all_ad_result.cpm,1) over (partition by all_ad_result.campaign_name order by all_ad_result.cpm)!=0 then round((all_ad_result.cpm / lag (all_ad_result.cpm,1) over (partition by all_ad_result.campaign_name order by all_ad_result.cpm))*100.0,4) end as cpm_diff,
	   case when lag (all_ad_result.ctr,1) over (partition by all_ad_result.campaign_name order by all_ad_result.ctr)!=0 then round((all_ad_result.ctr / lag (all_ad_result.sum_spend,1) over (partition by all_ad_result.campaign_name order by all_ad_result.ctr))*100.0,4) end as ctr_diff,
	   case when lag (all_ad_result.romi,1) over (partition by all_ad_result.campaign_name order by all_ad_result.romi)!=0 then round((all_ad_result.romi / lag (all_ad_result.romi,1) over (partition by all_ad_result.campaign_name order by all_ad_result.romi))*100.0,4) end as romi_diff
	   FROM all_ad_result
	   group by all_ad_result.ad_month, all_ad_result.campaign_name,all_ad_result.adset_name,all_ad_result.media_source,all_ad_result.utm_campaign, all_ad_result.sum_spend, all_ad_result.sum_impressions, all_ad_result.sum_clicks, all_ad_result.sum_value,all_ad_result.cpc, all_ad_result.cpm, all_ad_result.ctr, all_ad_result.romi;
	  
	   
