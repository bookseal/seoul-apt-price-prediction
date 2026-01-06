import streamlit as st
from src.io import load_sample_dataset
from src.plots import plot_median_price_by_district, plot_price_histogram


def display_district_analysis(df) -> None:
    """
    Visualize median prices across different districts in Seoul.

    Logic:
    District A (Median: 1.5B) vs District B (Median: 0.8B)
    ---> Clearly shows the 'Premium' of specific locations.
    """
    st.subheader("📍 2. Price by District")
    st.markdown(
        """
    Location is the most critical factor in real estate. 
    We use the **Median** price to represent each district, as it is less sensitive to extreme outliers than the Mean.
    """
    )
    fig = plot_median_price_by_district(df)
    st.plotly_chart(fig, use_container_width=True)


def display_price_distribution(df) -> None:
    """
    Visualize the spread of apartment prices.

    Infographic:
    [ Low Price ] <--- ( Most Transactions ) ---> [ High Price ]
          |                                            |
    ( Affordable )                               ( Luxury / Prime )
    """
    st.subheader("💰 1. Price Distribution")
    st.markdown(
        """
    This histogram shows how many apartments fall into different price ranges. 
    Usually, real estate data is **Right-Skewed**, meaning a few ultra-expensive apartments create a long tail.
    """
    )
    fig = plot_price_histogram(df)
    st.plotly_chart(fig, use_container_width=True)


def display_eda_header() -> None:
    """
    Render the page title and descriptive intro for EDA.

    Visual Guide:
    [ Overview ] -> [ Price Distribution ] -> [ Regional Analysis ]
    """
    st.title("📊 EDA: Discovering Patterns in Seoul")
    st.markdown(
        """
    Exploratory Data Analysis (EDA) is the process of 'listening' to what the data says before building a model. 
    We focus on two main questions:
    1. **How is the price distributed?** (Is it skewed?)
    2. **Which districts are the most expensive?** (Location impact)
    """
    )


def render_eda_page() -> None:
    """
    Main orchestrator for the EDA page.

    Flow:
    1. Load Sample -> 2. Header -> 3. Histogram -> 4. Bar Chart
    """
    try:
        df = load_sample_dataset()
        display_eda_header()

        # Section 1: Distribution
        display_price_distribution(df)

        st.divider()  # Horizontal Rule for readability

        # Section 2: Categorical Analysis
        display_district_analysis(df)

    except Exception as e:
        st.error(f"Failed to render EDA: {e}")


if __name__ == "__main__":
    render_eda_page()
