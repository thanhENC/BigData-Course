import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ML-RandomForest').getOrCreate()
from pyspark.ml import PipelineModel
pretrainRf = PipelineModel.load('rf_model')

st.title('DỰ ĐOÁN BÁN CHÉO')
st.write('''
# Bán chéo đeiii
---
''')

age = st.slider('Tuổi', 18, 100, 30)
vehicle_age = st.slider('Tuổi xe', 0.0, 5.0, 1.0, 0.5)
vehicle_damage = st.selectbox('Xe bị hư hỏng', ('Yes', 'No'))
previous_insurance = st.selectbox('Bảo hiểm trước đó', ('Yes', 'No'))
button = st.button("Dự đoán")

@st.cache(allow_output_mutation=True)
def get_model():
    rfModel = PipelineModel.load('Model/rf_model')
    return rfModel

rfModel = get_model()
result = {
    0.0: 'Không thể bán chéo',
    1.0: 'Có thể bán chéo'
}

def predict(model, input_df):
    predictions = model.transform(input_df)
    return predictions.select('prediction').collect()[0].prediction

def user_input_features():
    vehicle_age_0, vehicle_age_1, vehicle_age_2 = [0.0, 0.0, 0.0]
    if vehicle_age < 1.0:
        vehicle_age_0 = 1.0
    elif vehicle_age < 2.0:
        vehicle_age_1 = 1.0
    else:
        vehicle_age_2 = 1.0
    previously_insured = 1.0 if previous_insurance == 'Yes' else 0.0
    vehicle_damage = 1.0 if vehicle_damage == 'Yes' else 0.0
    # data = {'Age': age,
    #         'Vehicle_Age_0': vehicle_age_0,
    #         'Vehicle_Age_1': vehicle_age_1,
    #         'Vehicle_Age_2': vehicle_age_2,
    #         'Previously_Insured': 1.0 if previous_insurance == 'Yes' else 0.0,
    #         'Vehicle_Damage': 1.0 if vehicle_damage == 'Yes' else 0.0}
    data = {'features': [age, vehicle_age_0, vehicle_age_1, vehicle_age_2, previously_insured, vehicle_damage]}
    features = pd.DataFrame(data, index=[0])
    return spark.createDataFrame(features)

def main():
    if button:
        input_df = user_input_features()
        prediction = predict(rfModel, input_df)
        st.write('''
        # Kết quả
        ''')
        st.write(result[prediction])