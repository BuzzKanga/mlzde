if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print(f'Preprocessing: rows with passengers > 0: ', {(data['passenger_count'] > 0).sum()})
    print(f'Preprocessing: rows with trip distance > 0: ', {(data['trip_distance'] > 0).sum()})
    print(f'Preprocessing: where both above true: ', {((data['passenger_count'] > 0) & (data['trip_distance'] > 0)).sum()})
    values = data['VendorID'].unique().tolist()
    print(values)
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    dates = data['lpep_pickup_date'].nunique()
    print(dates)

    return data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output ['passenger_count'].isin([0]).sum() == 0, 'There are rides with 0 passengers'
