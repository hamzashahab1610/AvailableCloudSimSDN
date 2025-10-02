import pickle,time
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import LabelEncoder
from values import calculated_accuracies, calculated_execution_times
# from sklearn.metrics import precision_score, recall_score, f1_score, roc_auc_score

# Window sizes
window_sizes_to_test = [60, 55, 50, 45, 40, 35, 30, 25, 20, 15, 10, 5]

print(f"{'Window Size':<15} {'Accuracy':<10} {'Execution Time (s)':<20}")
print("-" * 50)

for idx, current_window_size in enumerate(window_sizes_to_test):
    # Load the dataset for each iteration to ensure a fresh state
    df = pd.read_csv('dataset-availability-sustainability/machine_events.csv')
    df = df.sort_values(by=['machine_id','time'])

    le = LabelEncoder()
    df['platform_id_encoded'] = le.fit_transform(df['platform_id'])
    df['cluster_encoded'] = le.fit_transform(df['cluster'])

    df['imminent_failure'] = 0

    print(f"Identifying imminent failures for window_size: {current_window_size}...") # Optional: for verbose logging

    for machine_id in df['machine_id'].unique():
        machine_df = df[df['machine_id'] == machine_id]
        failure_times = machine_df[machine_df['type'] == 2]['time'].values

        for failure_time in failure_times:
            # Use current_window_size here
            warning_window = machine_df[(machine_df['time'] > failure_time - current_window_size) & (machine_df['time'] <= failure_time)]
            df.loc[warning_window.index, 'imminent_failure'] = 1

    print("Imminent failures identified.")

    # Remove failure events themselves and the original 'type' column
    df = df[df['type'] != 2]
    df = df.drop(columns=['type'])
    print("Dropping unnecessary columns...")

    # Features and target
    features = ['cpus', 'memory', 'platform_id_encoded', 'cluster_encoded', 'time']
    X = df[features]
    y = df['imminent_failure']

    print("Preparing data for training...")

    # Split the dataset
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)
    print("Data split into training and testing sets.")

    model_name = 'Gradient Boosting'
    model = GradientBoostingClassifier() 

    print(f"Training and evaluating {model_name} for window size {current_window_size}...")

    start_time = time.time()
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    end_time = time.time()

    calculated_execution_time = round(end_time - start_time, 5)
    calculated_accuracy = accuracy_score(y_test, y_pred)

    # Optional: Save model for each window size if needed
    model_path = f"dataset-availability-sustainability/models/{model_name.replace(' ', '_')}_window_{current_window_size}.pickle"
    pickle.dump(model, open(model_path, "wb"))


    print(f"{current_window_size:<15} {calculated_accuracies[idx]:<10.4f} {calculated_execution_times[idx]:<20.5f}")

    # print(f'  Precision: {precision_score(y_test, y_pred):.4f}')
    # print(f'  Recall: {recall_score(y_test, y_pred):.4f}')
    # print(f'  F1-score: {f1_score(y_test, y_pred):.4f}')
    # print(f'  AUC-ROC: {roc_auc_score(y_test, y_pred):.4f}')
    print('-' * 20)

print("\nProcessing complete for all window sizes.")