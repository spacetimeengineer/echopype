"""test_convert_azfp.py

This module contains tests that:
- verify echopype converted files against those from AZFP Matlab scripts and EchoView
- convert AZFP file with different range settings across frequency
"""

import numpy as np
import pandas as pd
from scipy.io import loadmat
from echopype import open_raw
import pytest
from echopype.convert.parse_azfp import ParseAZFP


@pytest.fixture
def azfp_path(test_path):
    return test_path["AZFP"]


def check_platform_required_scalar_vars(echodata):
    # check convention-required variables in the Platform group
    for var in [
        "MRU_offset_x",
        "MRU_offset_y",
        "MRU_offset_z",
        "MRU_rotation_x",
        "MRU_rotation_y",
        "MRU_rotation_z",
        "position_offset_x",
        "position_offset_y",
        "position_offset_z",
    ]:
        assert var in echodata["Platform"]
        assert np.isnan(echodata["Platform"][var])


def test_convert_azfp_01a_matlab_raw(azfp_path):
    """Compare parsed raw data with Matlab outputs."""
    azfp_01a_path = azfp_path / '17082117.01A'
    azfp_xml_path = azfp_path / '17041823.XML'
    azfp_matlab_data_path = azfp_path / 'from_matlab/17082117_matlab_Data.mat'
    azfp_matlab_output_path = azfp_path / 'from_matlab/17082117_matlab_Output_Sv.mat'

    # Convert file
    echodata = open_raw(
        raw_file=azfp_01a_path, sonar_model='AZFP', xml_path=azfp_xml_path
    )

    # Read in the dataset that will be used to confirm working conversions. (Generated by Matlab)
    ds_matlab = loadmat(azfp_matlab_data_path)
    ds_matlab_output = loadmat(azfp_matlab_output_path)

    # Test beam group
    # frequency
    assert np.array_equal(
        ds_matlab['Data']['Freq'][0][0].squeeze(),
        echodata["Sonar/Beam_group1"].frequency_nominal / 1000,
    )  # matlab file in kHz
    # backscatter count
    assert np.array_equal(
        np.array(
            [ds_matlab_output['Output'][0]['N'][fidx] for fidx in range(4)]
        ),
        echodata["Sonar/Beam_group1"].backscatter_r.values,
    )

    # Test vendor group
    # Test temperature
    assert np.array_equal(
        np.array([d[4] for d in ds_matlab['Data']['Ancillary'][0]]).squeeze(),
        echodata["Vendor_specific"].ancillary.isel(ancillary_len=4).values,
    )
    assert np.array_equal(
        np.array([d[0] for d in ds_matlab['Data']['BatteryTx'][0]]).squeeze(),
        echodata["Vendor_specific"].battery_tx,
    )
    assert np.array_equal(
        np.array(
            [d[0] for d in ds_matlab['Data']['BatteryMain'][0]]
        ).squeeze(),
        echodata["Vendor_specific"].battery_main,
    )
    # tilt x-y
    assert np.array_equal(
        np.array([d[0] for d in ds_matlab['Data']['Ancillary'][0]]).squeeze(),
        echodata["Vendor_specific"].tilt_x_count,
    )
    assert np.array_equal(
        np.array([d[1] for d in ds_matlab['Data']['Ancillary'][0]]).squeeze(),
        echodata["Vendor_specific"].tilt_y_count,
    )

    # check convention-required variables in the Platform group
    check_platform_required_scalar_vars(echodata)


@pytest.mark.skip(reason="tests for comparing AZFP converted data with Matlab outputs have not been implemented")
def test_convert_azfp_01a_matlab_derived():
    """Compare variables derived from raw parsed data with Matlab outputs."""
    # TODO: test derived data
    #  - ds_beam.ping_time from 01A raw data records
    #  - investigate why ds_beam.tilt_x/y are different from ds_matlab['Data']['Tx']/['Ty']
    #  - derived temperature

    # # check convention-required variables in the Platform group
    # check_platform_required_scalar_vars(echodata)


def test_convert_azfp_01a_raw_echoview(azfp_path):
    """Compare parsed power data (count) with csv exported by EchoView."""
    azfp_01a_path = azfp_path / '17082117.01A'
    azfp_xml_path = azfp_path / '17041823.XML'

    # Read csv files exported by EchoView
    azfp_csv_path = [
        azfp_path / f"from_echoview/17082117-raw{freq}.csv"
        for freq in [38, 125, 200, 455]
    ]
    channels = []
    for file in azfp_csv_path:
        channels.append(
            pd.read_csv(file, header=None, skiprows=[0]).iloc[:, 6:]
        )
    test_power = np.stack(channels)

    # Convert to netCDF and check
    echodata = open_raw(
        raw_file=azfp_01a_path, sonar_model='AZFP', xml_path=azfp_xml_path
    )
    assert np.array_equal(test_power, echodata["Sonar/Beam_group1"].backscatter_r)

    # check convention-required variables in the Platform group
    check_platform_required_scalar_vars(echodata)


def test_convert_azfp_01a_different_ranges(azfp_path):
    """Test converting files with different range settings across frequency."""
    azfp_01a_path = azfp_path / '17031001.01A'
    azfp_xml_path = azfp_path / '17030815.XML'

    # Convert file
    echodata = open_raw(
        raw_file=azfp_01a_path, sonar_model='AZFP', xml_path=azfp_xml_path
    )
    assert echodata["Sonar/Beam_group1"].backscatter_r.sel(channel='55030-125-1').dropna(
        'range_sample'
    ).shape == (360, 438)
    assert echodata["Sonar/Beam_group1"].backscatter_r.sel(channel='55030-769-4').dropna(
        'range_sample'
    ).shape == (360, 135)

    # check convention-required variables in the Platform group
    check_platform_required_scalar_vars(echodata)


@pytest.mark.skip(reason="required pulse length not in Sv offset dictionary")
def test_convert_azfp_01a_no_temperature_pressure_tilt(azfp_path):
    """Test converting file with no valid temperature, pressure and tilt data."""

    azfp_01a_path = azfp_path / 'rutgers_glider_notemperature/22052500.01A'
    azfp_xml_path = azfp_path / 'rutgers_glider_notemperature/22052501.XML'

    echodata = open_raw(
        raw_file=azfp_01a_path, sonar_model='AZFP', xml_path=azfp_xml_path
    )

    # Temperature and pressure variables are not present in the Environment group
    assert "temperature" not in echodata["Environment"]
    assert "pressure" not in echodata["Environment"]

    # Tilt variables are present in the Platform group and their values are all nan
    assert "tilt_x" in echodata["Platform"]
    assert "tilt_y" in echodata["Platform"]
    assert echodata["Platform"]["tilt_x"].isnull().all()
    assert echodata["Platform"]["tilt_y"].isnull().all()


def test_convert_azfp_01a_pressure_temperature(azfp_path):
    """Test converting file with valid pressure and temperature data."""
    azfp_01a_path = azfp_path / 'pressure/22042221.01A'
    azfp_xml_path = azfp_path / 'pressure/22042220.XML'

    echodata = open_raw(
        raw_file=azfp_01a_path, sonar_model='AZFP', xml_path=azfp_xml_path
    )

    # Pressure variable is present in the Environment group and its values are not all nan
    assert "pressure" in echodata["Environment"]
    assert not echodata["Environment"]["pressure"].isnull().all()

    # Temperature variable is present in the Environment group and its values are not all nan
    assert "temperature" in echodata["Environment"]
    assert not echodata["Environment"]["temperature"].isnull().all()


def test_load_parse_azfp_xml(azfp_path):
    azfp_xml_path = azfp_path / '23081211.XML'
    parseAZFP = ParseAZFP(None, str(azfp_xml_path), "", "")
    parseAZFP.load_AZFP_xml()
    expected_params = ['instrument_type_string', 'instrument_type', 'major', 'minor', 'date',
                       'program_name', 'program', 'CPU', 'serial_number', 'board_version',
                       'file_version', 'parameter_version', 'configuration_version', 'backplane',
                       'delay_transmission_string', 'delay_transmission', 'eclock',
                       'digital_board_version', 'sensors_flag_pressure_sensor_installed',
                       'sensors_flag_paros_installed', 'sensors_flag', 'U0', 'Y1', 'Y2', 'Y3',
                       'C1', 'C2', 'C3', 'D1', 'D2', 'T1', 'T2', 'T3', 'T4', 'T5', 'X_a', 'X_b',
                       'X_c', 'X_d', 'Y_a', 'Y_b', 'Y_c', 'Y_d', 'period', 'ppm_offset',
                       'calibration', 'a0', 'a1', 'a2', 'a3', 'ka', 'kb', 'kc', 'A', 'B', 'C',
                       'num_freq', 'kHz_units', 'kHz', 'TVR', 'num_vtx', 'VTX0', 'VTX1', 'VTX2',
                       'VTX3', 'BP', 'EL', 'DS', 'min_pulse_len', 'sound_speed',
                       'start_date_svalue', 'start_date', 'num_frequencies', 'num_phases',
                       'data_output_svalue', 'data_output', 'frequency_units', 'frequency',
                       'phase_number', 'start_date_svalue_phase1', 'start_date_phase1',
                       'phase_type_svalue_phase1', 'phase_type_phase1', 'duration_svalue_phase1',
                       'duration_phase1', 'ping_period_units_phase1', 'ping_period_phase1',
                       'burst_interval_units_phase1', 'burst_interval_phase1',
                       'pings_per_burst_units_phase1', 'pings_per_burst_phase1',
                       'average_burst_pings_units_phase1', 'average_burst_pings_phase1',
                       'frequency_number_phase1', 'acquire_frequency_units_phase1',
                       'acquire_frequency_phase1', 'pulse_len_units_phase1', 'pulse_len_phase1',
                       'dig_rate_units_phase1', 'dig_rate_phase1', 'range_samples_units_phase1',
                       'range_samples_phase1', 'range_averaging_samples_units_phase1',
                       'range_averaging_samples_phase1', 'lock_out_index_units_phase1',
                       'lock_out_index_phase1', 'gain_units_phase1', 'gain_phase1',
                       'storage_format_units_phase1', 'storage_format_phase1',
                       'start_date_svalue_phase2', 'start_date_phase2', 'phase_type_svalue_phase2',
                       'phase_type_phase2', 'duration_svalue_phase2', 'duration_phase2',
                       'ping_period_units_phase2', 'ping_period_phase2',
                       'burst_interval_units_phase2', 'burst_interval_phase2',
                       'pings_per_burst_units_phase2', 'pings_per_burst_phase2',
                       'average_burst_pings_units_phase2', 'average_burst_pings_phase2',
                       'frequency_number_phase2', 'acquire_frequency_units_phase2',
                       'acquire_frequency_phase2', 'pulse_len_units_phase2', 'pulse_len_phase2',
                       'dig_rate_units_phase2', 'dig_rate_phase2', 'range_samples_units_phase2',
                       'range_samples_phase2', 'range_averaging_samples_units_phase2',
                       'range_averaging_samples_phase2', 'lock_out_index_units_phase2',
                       'lock_out_index_phase2', 'gain_units_phase2', 'gain_phase2',
                       'storage_format_units_phase2', 'storage_format_phase2', 'rt_version',
                       'rt_frequency', 'enabled', 'direction', 'water_depth_high_tide',
                       'instrument_depth_high_tide']
    assert set(parseAZFP.parameters.keys()) == set(expected_params)
    assert list(set(parseAZFP.parameters['instrument_type_string']))[0] == 'AZFP'
    assert isinstance(parseAZFP.parameters['num_freq'], int)
    assert parseAZFP.parameters['num_freq'] == 4
    assert parseAZFP.parameters['kHz'] == [67, 120, 200, 455]

    expected_len_params = ['acquire_frequency', 'pulse_len', 'dig_rate',
                           'range_samples', 'range_averaging_samples',
                           'lock_out_index', 'gain', 'storage_format']
    for num in parseAZFP.parameters["phase_number"]:
        assert isinstance(parseAZFP.parameters[f"pulse_len_phase{num}"], list)
        assert len(parseAZFP.parameters[f"acquire_frequency_phase{num}"]) == 4
        assert all(len(parseAZFP.parameters[f"{x}_phase{num}"]) == 4 for x in expected_len_params)
        assert parseAZFP.parameters[f"frequency_number_phase{num}"] == ['1', '2', '3', '4']
        assert parseAZFP.parameters[f"acquire_frequency_phase{num}"] == [1, 1, 1, 1]
        assert parseAZFP.parameters[f"dig_rate_phase{num}"] == [20000, 20000, 20000, 20000]
        assert parseAZFP.parameters[f"range_averaging_samples_phase{num}"] == [1, 1, 1, 1]
        assert parseAZFP.parameters[f"lock_out_index_phase{num}"] == [0, 0, 0, 0]
        assert parseAZFP.parameters[f"gain_phase{num}"] == [1, 1, 1, 1]
        assert parseAZFP.parameters[f"storage_format_phase{num}"] == [0, 0, 0, 0]
    assert parseAZFP.parameters['pulse_len_phase1'] == [1000, 1000, 1000, 1000]
    assert parseAZFP.parameters['pulse_len_phase2'] == [0, 0, 0, 0]
    assert parseAZFP.parameters['range_samples_phase1'] == [8273, 8273, 8273, 8273]
    assert parseAZFP.parameters['range_samples_phase2'] == [2750, 2750, 2750, 2750]
