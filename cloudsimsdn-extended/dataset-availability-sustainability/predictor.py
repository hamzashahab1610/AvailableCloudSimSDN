import pickle,time
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.tree import DecisionTreeClassifier
# from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
from sklearn.preprocessing import LabelEncoder

df = pd.read_csv('dataset-availability-sustainability/machine_events.csv')

df = df.sort_values(by=['machine_id','time'])

le = LabelEncoder()
df['platform_id_encoded'] = le.fit_transform(df['platform_id'])
df['cluster_encoded'] = le.fit_transform(df['cluster'])

window_size = 60

df['imminent_failure'] = 0

print("Identifying imminent failures...")

for machine_id in df['machine_id'].unique():
    machine_df = df[df['machine_id'] == machine_id]
    
    failure_times = machine_df[machine_df['type'] == 2]['time'].values

    for failure_time in failure_times:
        warning_window = machine_df[(machine_df['time'] > failure_time - window_size) & (machine_df['time'] <= failure_time)]
        
        df.loc[warning_window.index, 'imminent_failure'] = 1

print("Imminent failures identified.")

df = df[df['type'] != 2]
df = df.drop(columns=['type'])

print("Dropping unnecessary columns...")

print(df.head())

# Features and target
features = ['cpus', 'memory', 'platform_id_encoded', 'cluster_encoded', 'time']
X = df[features]
y = df['imminent_failure']

print("Preparing data for training...")

# Split the dataset
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

print("Data split into training and testing sets.")

models = [
    # ('Decision Tree', DecisionTreeClassifier()),
    # ('Random Forest', RandomForestClassifier()),
    ('Gradient Boosting', GradientBoostingClassifier()),
    # ('Logistic Regression', LogisticRegression(solver='lbfgs', max_iter=1000, class_weight='balanced'))
]

print("Training the models...")

for name, model in models:
    # Train the model
    print(f"Training and evaluating {name}...")

    start_time = time.time()

    model.fit(X_train, y_train)    

    # Evaluate the model
    y_pred = model.predict(X_test)

    end_time = time.time()

    execution_time = round(end_time - start_time, 2)

    # Save model
    model_path = f"dataset-availability-sustainability/models/{name}.pickle"
    pickle.dump(model, open(model_path, "wb"))

    print(f'Accuracy: {accuracy_score(y_test, y_pred):.4f}')
    print(f'Precision: {precision_score(y_test, y_pred):.4f}')
    print(f'Recall: {recall_score(y_test, y_pred):.4f}')
    print(f'F1-score: {f1_score(y_test, y_pred):.4f}')
    print(f'AUC-ROC: {roc_auc_score(y_test, y_pred):.4f}')
    print(f'Execution Time (s): {execution_time:.2f}')

    # class_report = classification_report(y_test, y_pred)
    # print(f'Classification Report:\n{class_report}')

    print('-' * 20)

print("Models trained and saved successfully.")