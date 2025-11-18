PLMP (Product Library Management Portal)
Overview

PLMP is a backend application designed to facilitate product management for businesses. This project provides RESTful APIs for managing products, orders, and users, streamlining the e-commerce experience. Built using Django and MongoEngine, it leverages the power of a NoSQL database to efficiently handle diverse product data.
Features

User authentication and authorization
Product management (CRUD operations)
Order processing and tracking
Role-based access control
RESTful API design
Integration with Shopify platform

Technologies Used

Django: A high-level Python web framework.
MongoEngine: An Object-Document Mapper (ODM) for working with MongoDB in Python.
JWT: JSON Web Token for authentication.
dotenv: For environment variable management.

Getting Started
Prerequisites

    Python (3.8.10 or higher)
    MongoDB
    pip (Python package installer)

Installation

    Clone the repository:

    bash

git clone https://github.com/KM-Digicommerce/back-end-amish.git

Navigate to the project directory:

bash

cd back-end-amish

Create a virtual environment (optional but recommended):

bash

python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`

Install the required packages:

bash

pip install -r requirements.txt

Set up environment variables:

Create a .env file in the root of the project directory and define the necessary environment variables, such as:

bash

MONGODB_URI=mongodb://localhost:27017/your_database
SECRET_KEY=your_secret_key

Run the migrations (if applicable):

bash

python manage.py migrate

Start the development server:

bash

python manage.py runserver
