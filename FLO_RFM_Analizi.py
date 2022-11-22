
###############################################################
# FLO RFM Analizi
###############################################################


#Online ayakkabı mağazası olan FLO müşterilerini segmentlere ayırıp
# bu segmentlere göre pazarlama stratejileri belirlemek istiyor.
# Buna yönelik olarak müşterilerin davranışları tanımlanacak
# bu davranışlardaki öbeklenmelere göre gruplar oluşturulacak.


###############################################################
#  Veri Seti Hikayesi
###############################################################

# Veri seti Flo’dan son alışverişlerini 2020 - 2021 yıllarında
# OmniChannel (hem online hem offline alışveriş yapan) olarak yapan
# müşterilerin geçmiş alışveriş davranışlarından elde edilen bilgilerden oluşmaktadır.

# Değişkenler
#12 Değişken 19.945 Gözlem
#master_id Eşsiz müşteri numarası
#order_channel Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile)
#last_order_channel En son alışverişin yapıldığı kanal
#first_order_date Müşterinin yaptığı ilk alışveriş tarihi
#last_order_date Müşterinin yaptığı son alışveriş tarihi
#last_order_date_online Müşterinin online platformda yaptığı son alışveriş tarihi
#last_order_date_offline Müşterinin offline platformda yaptığı son alışveriş tarihi
#order_num_total_ever_online Müşterinin online platformda yaptığı toplam alışveriş sayısı
#order_num_total_ever_offline Müşterinin offline'da yaptığı toplam alışveriş sayısı
#customer_value_total_ever_offline Müşterinin offline alışverişlerinde ödediği toplam ücret
#customer_value_total_ever_online Müşterinin online alışverişlerinde ödediği toplam ücret
#interested_in_categories_12 Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

###############################################################
# PROJE Görevleri
###############################################################

#############################################
# GÖREV 1:Veriyi Anlama ve Hazırlama
#############################################

# Adım1: flo_data_20K.csv verisini okuyunuz.Dataframe’in kopyasını oluşturunuz.

import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

df_ = pd.read_csv("datasets/flo_data_20k.csv")
df = df_.copy()
df.head()

#Adım2: Veri setinde
#a. İlk 10 gözlem,

df.head(10)

#b. Değişken isimleri,

df.columns

#c. Betimsel istatistik,

df.shape
df.describe().T

#d. Boş değer,

df.isnull().sum()

#e. Değişken tipleri, incelemesi yapınız.

df.info()
df["order_channel"].nunique()
df["master_id"].nunique()
df["order_channel"].value_counts()

#Adım3: Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Her bir müşterinin toplam
#alışveriş sayısı ve harcaması için yeni değişkenler oluşturunuz.

df["TotalOrderNum"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
df["TotalPrice"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]

#Adım4: Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.

date_columns = df.columns[df.columns.str.contains("date")]
df[date_columns] = df[date_columns].apply(pd.to_datetime)
df.info()

#Adım5: Alışveriş kanallarındaki müşteri sayısının, toplam alınan ürün sayısının ve toplam harcamaların dağılımına bakınız.

df.groupby("order_channel").agg({"master_id":"count",
                                 "TotalOrderNum":"sum",
                                 "TotalPrice":"sum"})

#Adım6: En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.

df.sort_values("TotalPrice",ascending=False)[:10]

#Adım7: En fazla siparişi veren ilk 10 müşteriyi sıralayınız.

df.sort_values("TotalOrderNum",ascending=False)[:10]

#Adım8: Veri ön hazırlık sürecini fonksiyonlaştırınız.

def data_func(df,csv=False):
    df["TotalPrice"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]
    df["TotalOrderNum"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
    date_columns = df.columns[df.columns.str.contains("date")]
    df[date_columns] = df[date_columns].apply(pd.to_datetime)

    return  df
data_func(df).head(10)

#############################################
# GÖREV 2: RFM Metriklerinin Hesaplanması
#############################################
#!recency değerini hesaplamak için analiz tarihini maksimum tarihten 2 gün sonrası seçebilirsiniz
#Adım 1: Recency, Frequency ve Monetary tanımlarını yapınız.
#Adım 2: Müşteri özelinde Recency, Frequency ve Monetary metriklerini hesaplayınız.
#Adım 3: Hesapladığınız metrikleri rfm isimli bir değişkene atayınız.

df["last_order_date"].max()
today_date = dt.datetime(2021, 6, 1)
type(today_date)

rfm = df.groupby("master_id").agg({'last_order_date':lambda last_order_date:(today_date - last_order_date.max()),
                                   'TotalOrderNum':lambda TotalOrderNum:TotalOrderNum.sum(),
                                   'TotalPrice': lambda TotalPrice:TotalPrice.sum()})
rfm.head()

#Adım 4: Oluşturduğunuz metriklerin isimlerini recency, frequency ve monetary olarak değiştiriniz.

rfm.columns = ["recency","frequency","monetary"]
rfm.head()
#############################################
# GÖREV 3: RF Skorunun Hesaplanması
#############################################
#Adım 1: Recency, Frequency ve Monetary metriklerini qcut yardımı ile 1-5 arasında skorlara çeviriniz.
#Adım 2: Bu skorları recency_score, frequency_score ve monetary_score olarak kaydediniz.

rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])

rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])

rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])


#Adım 3: recency_score ve frequency_score’u tek bir değişken olarak ifade ediniz ve RF_SCORE olarak kaydediniz.


rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str))
rfm.describe().T

#############################################
# GÖREV 4: RF Skorunun Segment Olarak Tanımlanması
#############################################
#Adım 1: Oluşturulan RF skorları için segment tanımlamaları yapınız.

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

#Adım 2: Aşağıdaki seg_map yardımı ile skorları segmentlere çeviriniz.

rfm['segment'] = rfm['RFM_SCORE'].replace(seg_map, regex=True)

#############################################
# GÖREV 5: Aksiyon Zamanı
#############################################
#Adım1: Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.

rfm[["segment","recency","frequency","monetary"]].groupby("segment").agg(["mean","count"])

#Adım2: RFM analizi yardımıyla aşağıda verilen 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv olarak kaydediniz.

#a.FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri
#tercihlerinin üstünde. Bu nedenle markanın tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak
#iletişime geçmek isteniliyor. Sadık müşterilerinden(champions, loyal_customers) ve kadın kategorisinden alışveriş
#yapan kişiler özel olarak iletişim kurulacak müşteriler. Bu müşterilerin id numaralarını csv dosyasına kaydediniz.

segmentsecme = rfm[(rfm["segment"] == "champions" ) | (rfm["segment"] == "loyal_customers")]
segmentsecme.shape[0]
kategorisecme = df[df["interested_in_categories_12"].str.contains("KADIN")]
kategorisecme.shape[0]
a = pd.merge(segmentsecme,kategorisecme[["interested_in_categories_12","master_id"]],on =["master_id"])
a.to_csv("new_customer.csv")

#b.Erkek ve Çocuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte
#iyi müşteri olan ama uzun süredir alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni
#gelen müşteriler özel olarak hedef alınmak isteniyor. Uygun profildeki müşterilerin id'lerini csv dosyasına kaydediniz.

segmentsecme1 = rfm[(rfm["segment"] == "new_customers" ) | (rfm["segment"] == "about_to_sleep") |(rfm["segment"] == "can_loose")]
segmentsecme1.shape[0]
kategorisecme1 = df[df["interested_in_categories_12"].str.contains("ERKEK|COCUK")]
kategorisecme1.shape[0]
b = segmentsecme1.merge(kategorisecme1[["interested_in_categories_12","master_id"]],on ="master_id",how="left")
b.to_csv("new_customer1.csv")


