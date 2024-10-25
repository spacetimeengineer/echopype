import os
import re
from typing import TYPE_CHECKING, Any, Callable, Dict, Union
import xarray as xr

from fsspec.mapping import FSMap
from typing_extensions import Literal

from .utils.log import _init_logger
from .utils import io

from .convert.parse_ad2cp import ParseAd2cp
from .convert.parse_azfp import ParseAZFP
from .convert.parse_azfp6 import ParseAZFP6
from .convert.parse_ek60 import ParseEK60
from .convert.parse_ek80 import ParseEK80
from .convert.set_groups_ad2cp import SetGroupsAd2cp
from .convert.set_groups_azfp import SetGroupsAZFP
from .convert.set_groups_azfp6 import SetGroupsAZFP6
from .convert.set_groups_ek60 import SetGroupsEK60
from .convert.set_groups_ek80 import SetGroupsEK80
from .echodata.simrad import check_input_args_combination


logger = _init_logger(__name__)

if TYPE_CHECKING:
    # Please keep SonarModelsHint updated with the keys of the SONAR_MODELS dict
    SonarModelsHint = Literal["AZFP", "AZFP6", "EK60", "ES70", "EK80", "ES80", "EA640", "AD2CP"]
    PathHint = Union[str, os.PathLike, FSMap]
    FileFormatHint = Literal[".nc", ".zarr"]
    EngineHint = Literal["netcdf4", "zarr"]


def validate_azfp_ext(test_ext: str):
    if not re.fullmatch(r"\.\d{2}[a-zA-Z]", test_ext):
        raise ValueError(
            'Expecting a file in the form ".XXY" '
            f"where XX is a number and Y is a letter but got {test_ext}"
        )


def validate_ext(ext: str) -> Callable[[str], None]:
    def inner(test_ext: str):
        if ext.casefold() != test_ext.casefold():
            raise ValueError(f"Expecting a {ext} file but got {test_ext}")

    return inner


def mod_Ex80(beam, sound_speed):
        mod = sound_speed * beam["transmit_duration_nominal"] / 4
        if isinstance(mod, xr.DataArray) and "time1" in mod.coords:
            mod = mod.squeeze().drop_vars("time1")
        return mod
    


def mod_Ex60(beam, sound_speed):
    # 2-sample shift in the beginning
    return 2 * beam["sample_interval"] * sound_speed / 2  # [frequency x range_sample]

def range_meter_Ex80(beam, vend, sound_speed):    
    range_meter = range_meter - mod_Ex80(beam, sound_speed)

    # Change range for all channels with GPT
    if "GPT" in vend["transceiver_type"]:
        ch_GPT = vend["transceiver_type"] == "GPT"
        range_meter.loc[dict(channel=ch_GPT)] = range_meter.sel(channel=ch_GPT) - mod_Ex60(beam, sound_speed)
        
def EK80_waveform_mode_check(waveform_mode, encode_mode):
    if waveform_mode is None:
        raise ValueError("The waveform_mode must be specified for EK80 calibration")
    
def EK80_encode_mode_check(encode_mode):
    if encode_mode is None:
        raise ValueError("The encode_mode must be specified for EK80 calibration")
    
def EK80_mode_checks(waveform_mode, encode_mode):
    EK80_waveform_mode_check(waveform_mode)
    EK80_encode_mode_check(encode_mode)
    check_input_args_combination(waveform_mode=waveform_mode, encode_mode=encode_mode)

        
def EK60_waveform_mode_check(waveform_mode, encode_mode):
    if waveform_mode is not None and waveform_mode != "CW":
        logger.warning(
            "This sonar model transmits only narrowband signals (waveform_mode='CW'). "
            "Calibration will be in CW mode",
        )
def EK60_encode_mode_check(encode_mode):
    if encode_mode is not None and encode_mode != "power":
        logger.warning(
            "This sonar model only record data as power or power/angle samples "
            "(encode_mode='power'). Calibration will be done on the power samples.",
        )
    
def EK60_mode_checks(waveform_mode, encode_mode):
    EK60_waveform_mode_check(waveform_mode)
    EK60_encode_mode_check(encode_mode)

    
    
def AZFP_waveform_mode_check(waveform_mode):
    if waveform_mode is not None and waveform_mode != "CW":
        logger.warning(
            "This sonar model transmits only narrowband signals (waveform_mode='CW'). "
            "Calibration will be in CW mode",
        )
    
def AZFP_encode_mode_check(encode_mode):
    if encode_mode is not None and encode_mode != "power":
        logger.warning(
            "This sonar model only record data as power or power/angle samples "
            "(encode_mode='power'). Calibration will be done on the power samples.",
        )
    
def AZFP_mode_checks(waveform_mode, encode_mode):
    AZFP_waveform_mode_check(waveform_mode)
    AZFP_encode_mode_check(encode_mode)        

def EK80_add_attrs_check(cal_type, ds, waveform_mode, encode_mode):
    ds[cal_type] = ds[cal_type].assign_attrs(
        {
            "waveform_mode": waveform_mode,
            "encode_mode": encode_mode,
        }
    )
    
def EK60_backscatter_check(echodata, ed_beam_group, waveform_mode, encode_mode):
    return echodata["Sonar/Beam_group1"]["backscatter_r"].nbytes
    
def AZFP_backscatter_check(echodata, ed_beam_group, waveform_mode, encode_mode):
    return echodata["Sonar/Beam_group1"]["backscatter_r"].nbytes
    
def EK80_backscatter_check(echodata, ed_beam_group, waveform_mode, encode_mode):
    # Select source of backscatter data
    beam = echodata[ed_beam_group]

    # Go through waveform and encode cases
    if (waveform_mode == "BB") or (
        waveform_mode == "CW" and encode_mode == "complex"
    ):
        total_nbytes = beam["backscatter_r"].nbytes + beam["backscatter_i"].nbytes
    if waveform_mode == "CW" and encode_mode == "power":
        total_nbytes = beam["backscatter_r"].nbytes
                
def AD2CP_save_file_check(echodata, output_path, engine, compress, COMPRESSION_SETTINGS, BEAM_SUBGROUP_DEFAULT, **kwargs):
    for i in range(1, len(echodata["Sonar"]["beam_group"]) + 1):
        io.save_file(
            echodata[f"Sonar/Beam_group{i}"],
            path=output_path,
            mode="a",
            engine=engine,
            group=f"Sonar/Beam_group{i}",
            compression_settings=COMPRESSION_SETTINGS[engine] if compress else None,
            **kwargs,
        )
def BASE_save_file_check(echodata, output_path, engine, compress, COMPRESSION_SETTINGS, BEAM_SUBGROUP_DEFAULT, **kwargs):
    io.save_file(
        echodata[f"Sonar/{BEAM_SUBGROUP_DEFAULT}"],
        path=output_path,
        mode="a",
        engine=engine,
        group=f"Sonar/{BEAM_SUBGROUP_DEFAULT}",
        compression_settings=COMPRESSION_SETTINGS[engine] if compress else None,
        **kwargs,
    )
    if echodata["Sonar/Beam_group2"] is not None:
        # some sonar model does not produce Sonar/Beam_group2
        io.save_file(
            echodata["Sonar/Beam_group2"],
            path=output_path,
            mode="a",
            engine=engine,
            group="Sonar/Beam_group2",
            compression_settings=COMPRESSION_SETTINGS[engine] if compress else None,
            **kwargs,
        )


           
SONAR_MODELS: Dict["SonarModelsHint", Dict[str, Any]] = {
    "AZFP": {
        "validate_ext": validate_azfp_ext,
        "xml": True,
        "accepts_bot": False,
        "accepts_idx": False,
        "parser": ParseAZFP,
        "parsed2zarr": None,
        "set_groups": SetGroupsAZFP,
        "mode_checks" : AZFP_mode_checks,
        "add_attrs_check" : lambda cal_type, ds, waveform_mode, encode_mode : None,
        "backscatter_check" : AZFP_backscatter_check,
        "save_file_check" : BASE_save_file_check
    },
    "AZFP6": {
        "validate_ext": validate_ext(".azfp"),
        "xml": False,
        "accepts_bot": False,
        "accepts_idx": False,
        "parser": ParseAZFP6,
        "parsed2zarr": None,
        "set_groups": SetGroupsAZFP6,
        "mode_checks" : lambda: None,
        "add_attrs_check" : lambda cal_type, ds, waveform_mode, encode_mode : None,
        "backscatter_check" : lambda echodata, ed_beam_group, waveform_mode, encode_mode : None,
        "save_file_check" : BASE_save_file_check
    },
    "EK60": {
        "validate_ext": validate_ext(".raw"),
        "xml": False,
        "accepts_bot": True,
        "accepts_idx": True,
        "parser": ParseEK60,
        "set_groups": SetGroupsEK60,
        "range_meter": lambda range_meter, beam, vend, sound_speed : range_meter - mod_Ex60(beam, sound_speed),
        "mode_checks" : EK60_mode_checks,
        "add_attrs_check" : lambda cal_type, ds, waveform_mode, encode_mode : None,
        "backscatter_check" : EK60_backscatter_check,
        "save_file_check" : BASE_save_file_check
    },
    "ES70": {
        "validate_ext": validate_ext(".raw"),
        "xml": False,
        "accepts_bot": False,
        "accepts_idx": False,
        "parser": ParseEK60,
        "set_groups": SetGroupsEK60,
        "range_meter": lambda range_meter, beam, vend, sound_speed : range_meter - mod_Ex60(beam, sound_speed),
        "mode_checks" : lambda: None,
        "add_attrs_check" : lambda cal_type, ds, waveform_mode, encode_mode : None,
        "backscatter_check" : lambda echodata, ed_beam_group, waveform_mode, encode_mode : None,
        "save_file_check" : BASE_save_file_check
    },
    "EK80": {
        "validate_ext": validate_ext(".raw"),
        "xml": False,
        "accepts_bot": True,
        "accepts_idx": True,
        "parser": ParseEK80,
        "set_groups": SetGroupsEK80,
        "range_meter": lambda range_meter, beam, vend, sound_speed : range_meter_Ex80(beam, vend, sound_speed),
        "mode_checks" : EK80_mode_checks,
        "add_attrs_check" : EK80_add_attrs_check,
        "backscatter_check" : EK80_backscatter_check,
        "save_file_check" : BASE_save_file_check
    },
    "ES80": {
        "validate_ext": validate_ext(".raw"),
        "xml": False,
        "accepts_bot": False,
        "accepts_idx": False,
        "parser": ParseEK80,
        "set_groups": SetGroupsEK80,
        "range_meter": lambda range_meter, beam, vend, sound_speed : range_meter_Ex80(beam, vend, sound_speed),
        "mode_checks" : lambda: None,
        "add_attrs_check" : lambda cal_type, ds, waveform_mode, encode_mode : None,
        "backscatter_check" : lambda echodata, ed_beam_group, waveform_mode, encode_mode : None,
        "save_file_check" : BASE_save_file_check
    },
    "EA640": {
        "validate_ext": validate_ext(".raw"),
        "xml": False,
        "accepts_bot": False,
        "accepts_idx": False,
        "parser": ParseEK80,
        "set_groups": SetGroupsEK80,
        "range_meter": lambda range_meter, beam, vend, sound_speed : range_meter - mod_Ex80(beam, sound_speed),
        "mode_checks" : lambda: None,
        "add_attrs_check" : lambda cal_type, ds, waveform_mode, encode_mode : None,
        "backscatter_check" : lambda echodata, ed_beam_group, waveform_mode, encode_mode : None,
        "save_file_check" : BASE_save_file_check
    },
    "AD2CP": {
        "validate_ext": validate_ext(".ad2cp"),
        "xml": False,
        "accepts_bot": False,
        "accepts_idx": False,
        "parser": ParseAd2cp,
        "parsed2zarr": None,
        "set_groups": SetGroupsAd2cp,
        "mode_checks" : lambda: None,
        "add_attrs_check" : lambda cal_type, ds, waveform_mode, encode_mode : None,
        "backscatter_check" : lambda echodata, ed_beam_group, waveform_mode, encode_mode : None,
        "save_file_check" : AD2CP_save_file_check
    },
}
