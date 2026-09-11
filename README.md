# Daily Expense Manager

A robust, web-based application built with Django to help users track their daily expenses, visualize spending habits, and manage personal finances efficiently.

## 📌 Features

* **User Authentication**: Secure Sign Up, Login, and Logout functionality to ensure data privacy. Users can only access their own data.
* **Expense Management (CRUD)**:
    * **Add Expenses**: Record expenses with date, amount, category, and optional description.
    * **View Dashboard**: See a list of expenses ordered by date.
    * **Edit & Delete**: Modify or remove existing expense entries.
* **Smart Filtering**: Filter expenses by **Category** (e.g., Food, Transport, Entertainment) and **Date Range** (Start Date to End Date).
* **Visual Analytics**: Interactive **Pie Chart** powered by Chart.js to visualize spending distribution across different categories.
* **Data Export**: Export expense history to **CSV** format for external analysis.
* **Account Management**: Users have the option to permanently delete their account and all associated data.
* **Responsive UI**: A clean, user-friendly interface styled with custom CSS and Font Awesome icons.

## 🛠️ Tech Stack

* **Backend**: Python 3.13, Django 5.2.6
* **Frontend**: HTML5, CSS3, JavaScript (Chart.js)
* **Database**: SQLite (Default)
* **Icons**: Font Awesome

## 🚀 Installation & Setup

Follow these steps to set up the project locally.

### Prerequisites

* Python 3.x installed
* pip (Python package manager)

### Steps

1.  **Clone the Repository**
    ```bash
    git clone <repository-url>
    cd expense_manager
    ```

2.  **Create a Virtual Environment (Optional but Recommended)**
    ```bash
    # Windows
    python -m venv venv
    venv\Scripts\activate

    # macOS/Linux
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install Dependencies**
    Ensure Django is installed (and any other requirements if a `requirements.txt` exists):
    ```bash
    pip install django
    ```

4.  **Apply Database Migrations**
    Initialize the SQLite database:
    ```bash
    python manage.py migrate
    ```

5.  **Run the Development Server**
    Start the application:
    ```bash
    python manage.py runserver
    ```

6.  **Access the Application**
    Open your web browser and navigate to:
    `http://127.0.0.1:8000/`

    ## Least-Connections gRPC Experiment

    This repository also contains a standalone load-balancing experiment. It uses
    `proto/worker.proto` so the existing expense workflow contract in
    `proto/service.proto` remains unchanged.

    Generate the worker stubs from the project root:

    ```powershell
    .\venv\Scripts\python.exe -m grpc_tools.protoc -I=proto --python_out=. --grpc_python_out=. proto/worker.proto
    ```

    Open four terminals with the virtual environment activated. Start one worker
    in each of the first three terminals:

    ```powershell
    python worker_server.py 60201
    python worker_server.py 60202
    python worker_server.py 60203
    ```

    After all workers report that they are listening, run the load balancer in the
    fourth terminal:

    ```powershell
    python load_balancer.py
    ```

    The balancer reserves the least-loaded backend before dispatching each request
    and releases that reservation in a `finally` block. Run its focused tests with:

    ```powershell
    python -m unittest -v test_load_balancer.py
    ```

    ## Redis Cache Gateway (Experiment 8)

    The cache gateway wraps the existing `grpc_server.py` without changing the
    backend. It caches `GetStatus` responses for 10 seconds and keeps a separate,
    never-expiring stale copy for backend outages.

    Install and start Redis, then install the Python client:

    ```powershell
    # Windows: install Redis using your preferred local Redis distribution
    .\venv\Scripts\python.exe -m pip install -r requirements.txt
    ```

    In one terminal, start the existing backend:

    ```powershell
    .\venv\Scripts\python.exe grpc_server.py
    ```

    In another terminal, exercise the gateway:

    ```powershell
    .\venv\Scripts\python.exe cache_gateway.py ExpenseManagerClient
    .\venv\Scripts\python.exe cache_gateway.py ExpenseManagerClient
    ```

    The first call should report `source=backend`; the second should report
    `source=cache-hit`. Stopping Redis makes the gateway call the backend directly.
    Stopping the backend while Redis is running makes it serve `source=stale-fallback`
    from the backup key. Run the focused tests with:

    ```powershell
    .\venv\Scripts\python.exe -m unittest -v test_cache_gateway.py
    ```

## 📂 Project Structure

```text
expense_manager/
│
├── expense_manager/      # Project configuration (settings, urls, wsgi/asgi)
│   ├── settings.py
│   └── urls.py
│
├── expenses/             # Main application logic
│   ├── migrations/       # Database migrations
│   ├── templates/        # HTML templates (login, dashboard, charts)
│   ├── admin.py          # Admin panel configuration
│   ├── forms.py          # Forms for expenses and user signup
│   ├── models.py         # Database models (Expense, User)
│   ├── urls.py           # App-specific URL routing
│   └── views.py          # View logic (filtering, export, CRUD)
│
├── db.sqlite3            # Database file
└── manage.py             # Django command-line utility
