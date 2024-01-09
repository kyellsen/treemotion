import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
import plotly.express as px
import plotly.graph_objects as go

from utils.log import get_logger

logger = get_logger(__name__)
import matplotlib.pyplot as plt


def linear_regression(df, col1, col2):
    """
    Calculate linear regression for two columns of a DataFrame.

    :param df: DataFrame containing the data.
    :param col1: Name of the first column.
    :param col2: Name of the second column.
    :return: Coefficients of the linear regression model.
    """
    logger.info(f'Calculating linear regression for {col1} and {col2}...')
    X = df[col1].values.reshape(-1, 1)
    y = df[col2].values.reshape(-1, 1)
    model = LinearRegression()
    model.fit(X, y)
    return model.coef_, model.intercept_


def polynomial_regression(df, col1, col2, degree=5):
    """
    Calculate polynomial regression for two columns of a DataFrame.

    :param df: DataFrame containing the data.
    :param col1: Name of the first column.
    :param col2: Name of the second column.
    :param degree: Degree of the polynomial.
    :return: Coefficients of the polynomial regression model.
    """
    logger.info(f'Calculating polynomial regression of degree {degree} for {col1} and {col2}...')
    X = df[col1].values.reshape(-1, 1)
    y = df[col2].values.reshape(-1, 1)
    model = make_pipeline(PolynomialFeatures(degree), LinearRegression())
    model.fit(X, y)
    return model.steps[1][1].coef_, model.steps[1][1].intercept_


def regression_plot_matplotlib(df, col1, col2, method='linear', degree=5):
    """
    Plot regression results for two columns of a DataFrame.

    :param df: DataFrame containing the data.
    :param col1: Name of the first column.
    :param col2: Name of the second column.
    :param method: Regression method, either 'linear' or 'polynomial'.
    :param degree: Degree of the polynomial (only used if method='polynomial').
    """
    logger.info(f'Plotting {method} regression for {col1} and {col2}...')
    df = df[[col1, col2]].dropna()  # Drop rows with NaN values
    X = df[col1].values.reshape(-1, 1)
    y = df[col2].values.reshape(-1, 1)

    plt.figure(figsize=(10, 6))
    plt.scatter(X, y, label='Data points')

    if method == 'linear':
        coef, intercept = linear_regression(df, col1, col2)
        y_fit = coef * X + intercept
        plt.plot(X, y_fit, color='red', label='Linear fit')
    elif method == 'polynomial':
        coef, intercept = polynomial_regression(df, col1, col2, degree)
        y_fit = np.polyval(np.flip(coef), X) + intercept
        plt.plot(X, y_fit, color='red', label=f'Polynomial fit of degree {degree}')

    plt.xlabel(col1)
    plt.ylabel(col2)
    plt.title(f'{method.capitalize()} Regression between {col1} and {col2}')
    plt.legend()
    plt.show()


# def regression_plot_plotly(df, col1, col2, method='linear', degree=2):
#     """
#     Plot regression results for two columns of a DataFrame.
#
#     :param df: DataFrame containing the data.
#     :param col1: Name of the first column.
#     :param col2: Name of the second column.
#     :param method: Regression method, either 'linear' or 'polynomial'.
#     :param degree: Degree of the polynomial (only used if method='polynomial').
#     :return: Plotly Figure object.
#     """
#     logger.info(f'Plotting {method} regression for {col1} and {col2}...')
#     df = df[[col1, col2]].dropna()
#     X = df[col1].values.reshape(-1, 1)
#     y = df[col2].values.reshape(-1, 1)
#     fig = px.scatter(df, x=col1, y=col2)
#     if method == 'linear':
#         coef, intercept = linear_regression(df, col1, col2)
#         y_fit = coef * X + intercept
#     elif method == 'polynomial':
#         coef, intercept = polynomial_regression(df, col1, col2, degree)
#         y_fit = np.polyval(np.flip(coef), X) + intercept
#     fig.add_trace(go.Scatter(x=df[col1], y=y_fit.reshape(-1), mode='lines', name=f'{method} fit'))
#     return fig

def plot_regression(df, column_x, column_y, degree):
    """
    Plot a regression based on the input DataFrame and specified columns.

    :param df: DataFrame containing the data.
    :param column_x: Name of the column to use as x-values.
    :param column_y: Name of the column to use as y-values.
    :param degree: Degree of the polynomial for the regression.
    :return: None.
    """
    # Extract data from the DataFrame
    x = df[column_x].sort_values()
    y = df[column_y]

    # Compute the coefficients of the polynomial regression
    coefficients = np.polyfit(x, y, degree)

    # Create a polynomial function with the computed coefficients
    poly = np.poly1d(coefficients)

    # Generate y-values for the regression line
    y_poly = poly(x)

    # Create the plot
    plt.figure(figsize=(10, 6))
    plt.plot(x, y, 'o', label='Original data')
    plt.plot(x, y_poly, 'r-', label='Fitted line')

    # Add labels and a legend
    plt.xlabel(column_x)
    plt.ylabel(column_y)
    plt.legend()

    # Display the plot
    plt.show()
