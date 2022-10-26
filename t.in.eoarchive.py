#!/usr/bin/env python3

############################################################################
#
# MODULE:       t.in.eoarchive
#
# AUTHOR(S):    Guido Riembauer
#
# PURPOSE:      Imports an EO collection from an archive as STRDS
#
#
# COPYRIGHT:	(C) 2022 by mundialis and the GRASS Development Team
#
#               This program is free software under the GNU General Public
#               License (>=v3). Read the file COPYING that comes with GRASS
#               for details.
#
#############################################################################

# %Module
# % description: Imports an EO collection from an archive as STRDS
# % keyword: temporal
# % keyword: satellite
# % keyword: Sentinel
# % keyword: import
# %end

# %option
# % key: start
# % type: string
# % required: no
# % multiple: no
# % description: Start date ('YYYY-MM-DD')
# % guisection: Filter
# %end

# %option
# % key: end
# % type: string
# % required: no
# % multiple: no
# % description: End date ('YYYY-MM-DD')
# % guisection: Filter
# %end

# %option
# % key: bands
# % type: string
# % required: no
# % multiple: yes
# % description: Bands to import
# % options: S2_B2,S2_B3,S2_B4,S2_B5,S2_B6,S2_B7,S2_B8,S2_B8A,S2_B11,S2_B12,S2_CLM
# % answer: S2_B4,S2_B8,S2_CLM
# % guisection: Filter
# %end

# %option G_OPT_MEMORYMB
# %end

# %option
# % key: wfs_mgrs
# % type: string
# % required: no
# % multiple: no
# % label: WFS for the Military Grid Reference System (MGRS, UTM grid system)
# % answer: https://geoserver.mundialis.de/geoserver/sentinel/wfs?
# %end

# %option
# % key: wfs_name
# % type: string
# % required: no
# % multiple: no
# % label: WFS layer name of the Military Grid Reference System (MGRS, UTM grid system)
# % answer: sentinel:mgrs
# %end

# %option G_OPT_STRDS_OUTPUT
# %end

# %option
# % key: collection
# % type: string
# % required: yes
# % multiple: no
# % label: EO-Lab data collection to import
# % description: Currently only S2-L2A-MAJA is supported
# % answer: S2-L2A-MAJA
# % options: S2-L2A-MAJA
# %end

# %option
# % key: archive
# % type: string
# % required: yes
# % multiple: no
# % label: eo archive to import from
# % description: Currently only eolab is supported
# % answer: eolab
# % options: eolab
# %end

# %option
# % key: mountpoint
# % type: string
# % required: yes
# % multiple: no
# % label: Name of the mountpoint where Sentinel data is mounted
# % answer: /codede
# %end

# %option G_OPT_M_NPROCS
# % description: Number of cores for multiprocessing, -2 is n_cores-1
# % answer: -2
# % guisection: Optional
# %end


import grass.script as grass
from multiprocessing import Pool
import atexit
from datetime import date, datetime
import multiprocessing as mp
import os
import psutil
import subprocess


# initialize global vars
rm_vectors = []
rm_rasters = []
TMPLOC = None
SRCGISRC = None
TGTGISRC = None
GISDBASE = None

eolab_collection_params = {
    "S2-L2A-MAJA": {
        "basepath": os.path.join("Sentinel-2", "MSI", "L2A-MAJA"),
        "file_format": ".tif",
        "bands_filesuffixes": {
            "S2_B2": "FRE_B2",
            "S2_B3": "FRE_B3",
            "S2_B4": "FRE_B4",
            "S2_B5": "FRE_B5",
            "S2_B6": "FRE_B6",
            "S2_B7": "FRE_B7",
            "S2_B8": "FRE_B8",
            "S2_B8A": "FRE_B8A",
            "S2_B11": "FRE_B11",
            "S2_B12": "FRE_B12",
            "S2_CLM": "CLM_R1"
        },
        "CLM_dir": "MASKS",
        "tile_system": "MGRS"
    }
}


def cleanup():
    nuldev = open(os.devnull, 'w')
    kwargs = {
        'flags': 'f',
        'quiet': True,
        'stderr': nuldev
    }
    for rmv in rm_vectors:
        if grass.find_file(name=rmv, element='vector')['file']:
            grass.run_command(
                'g.remove', type='vector', name=rmv, **kwargs)
    for rmrast in rm_rasters:
        if grass.find_file(name=rmrast, element='raster')['file']:
            grass.run_command(
                'g.remove', type='raster', name=rmrast, **kwargs)
    if TGTGISRC:
        os.environ['GISRC'] = str(TGTGISRC)
    # remove temp location
    if TMPLOC:
        grass.try_rmdir(os.path.join(GISDBASE, TMPLOC))
    if SRCGISRC:
        grass.try_remove(SRCGISRC)


def createTMPlocation(epsg=4326):
    """ Function that creates a TMP location and switches to it
    Args:
        epsg(int): EPSG code of the TMP location
    """
    global TMPLOC, SRCGISRC
    SRCGISRC = grass.tempfile()
    TMPLOC = 'temp_import_location_' + str(os.getpid())
    f = open(SRCGISRC, 'w')
    f.write('MAPSET: PERMANENT\n')
    f.write(f'GISDBASE: {GISDBASE}\n')
    f.write(f'LOCATION_NAME: {TMPLOC}\n')
    f.write('GUI: text\n')
    f.close()

    proj_test = grass.parse_command('g.proj', flags='g')
    if 'epsg' in proj_test:
        epsg_arg = {'epsg': epsg}
    else:
        epsg_arg = {'srid': "EPSG:{}".format(epsg)}
    # create temp location from input without import
    grass.verbose(_(f"Creating temporary location with EPSG:{epsg}..."))
    grass.run_command('g.proj', flags='c', location=TMPLOC, quiet=True,
                      **epsg_arg)

    # switch to temp location
    os.environ['GISRC'] = str(SRCGISRC)
    proj = grass.parse_command('g.proj', flags='g')
    if 'epsg' in proj:
        new_epsg = proj['epsg']
    else:
        new_epsg = proj['srid'].split('EPSG:')[1]
    if new_epsg != str(epsg):
        grass.fatal(_("Creation of temporary location failed!"))

    # return SRCGISRC, TMPLOC


def get_actual_location():
    """ Function that returns location and mapset of the current grass GISENV
    Returns:
        tgtloc (string): current location name
        tgtmapset (string): current mapset name

    """
    global TGTGISRC, GISDBASE
    # get actual location, mapset, ...
    grassenv = grass.gisenv()
    tgtloc = grassenv['LOCATION_NAME']
    tgtmapset = grassenv['MAPSET']
    GISDBASE = grassenv['GISDBASE']
    TGTGISRC = os.environ['GISRC']
    return tgtloc, tgtmapset


def get_utmcells():
    """ Function that queries the UTM Military Grid Reference System for Grid
        tiles that overlap with the current computational region
    Returns:
        utm_tiles (list): list of UTM tiles that overlap with the current
                          computational region

    """
    # save region
    tmpregionname = 'tmp_region_utmgrid_' + str(os.getpid())
    grass.run_command('v.in.region', output=tmpregionname, quiet=True)
    rm_vectors.append(tmpregionname)

    # get actual location, mapset, ...
    tgtloc, tgtmapset = get_actual_location()

    # create temporary location with epsg:4326
    epsg = 4326
    createTMPlocation(epsg)

    # import grid
    grass.run_command(
        'v.proj', location=tgtloc, mapset=tgtmapset, input=tmpregionname,
        output=tmpregionname, quiet=True)
    grass.run_command('g.region', vector=tmpregionname, quiet=True)
    grass.run_command(
        'v.in.wfs',
        url=options['wfs_mgrs'],
        output='tmp_s2tiles_area', srs=epsg, name=options['wfs_name'],
        flags='r', quiet=True)
    utm_tiles = [x for x in grass.parse_command(
        'v.db.select', map='tmp_s2tiles_area', column='name', flags='c')]

    # switch to target location
    os.environ['GISRC'] = str(TGTGISRC)

    return utm_tiles


def check_start_end(start, end):
    """ Checks the validity of the user defined start and end date
    Args:
        start(string): user-defined start date in format YYYY-MM-DD
        end(string): user-defined end date in format YYYY-MM-DD
    Returns:
        start_date(datetime): user-defined start date as datetime object
        end_date(datetime): user-defined end date as datetime object

    """
    # set end if end is today
    if end == "today":
        end = date.today().strftime("%Y-%m-%d")
    # check start after 2015-07
    start_list = [int(x) for x in start.split('-')]
    end_list = [int(x) for x in end.split('-')]
    try:
        start_date = date(start_list[0], start_list[1], start_list[2])
        end_date = date(end_list[0], end_list[1], end_list[2])
    except Exception as e:
        grass.fatal(_(f"Start/End date is not defined in format YYYY-MM-DD: {e}"))
    maja_date = date(2015, 7, 1)
    if end_date < maja_date:
        grass.fatal(_("End is before 2015-07-01, please select a later "
                      "end date, as no data is available otherwise"))
    # check if end ist after start
    if end_date < start_date:
        grass.fatal(_("End date is before start date"))

    return start_date, end_date


def test_nprocs_memory():
    """ Checks the available number of cores and memory
    Returns:
        nprocs_real(int): available number of cores
        used_ram(int): available memory in MB RAM

    """
    # Test nprocs settings
    nprocs = int(options['nprocs'])
    if nprocs == -2:
        nprocs_real = mp.cpu_count()-1
    else:
        nprocs_real = mp.cpu_count()
        if nprocs > nprocs_real:
            grass.warning(_(f"Using {nprocs} parallel processes "
                            f"but only {nprocs_real} CPUs available."))
    # check memory
    memory = int(options['memory'])
    free_ram = freeRAM('MB', 100)
    if free_ram < memory:
        grass.warning(_(f"Using {memory} MB but only "
                        f"{free_ram} MB RAM available."))
        grass.warning(_(f"Set used memory to {free_ram} MB."))
        used_ram = free_ram
    else:
        used_ram = memory
    return nprocs_real, used_ram


def freeRAM(unit, percent=100):
    """ Function that returns the available RAM.
    Args:
        unit(string): 'GB' or 'MB'
        percent(int): number of percent which shoud be used of the free RAM
                      default 100%
    Returns:
        memory_MB_percent/memory_GB_percent(int): percent of the free RAM in
                                                  MB or GB

    """
    # use psutil cause of alpine busybox free version for RAM/SWAP usage
    mem_available = psutil.virtual_memory().available
    swap_free = psutil.swap_memory().free
    memory_GB = (mem_available + swap_free) / 1024.0**3
    memory_MB = (mem_available + swap_free) / 1024.0**2

    if unit == "MB":
        memory_MB_percent = memory_MB * percent / 100.0
        return int(round(memory_MB_percent))
    elif unit == "GB":
        memory_GB_percent = memory_GB * percent / 100.0
        return int(round(memory_GB_percent))
    else:
        grass.fatal(_(f"Memory unit <{unit}> not supported"))


def browse_eolab_collection(collection, bands, mountpoint, start_date, end_date):
    """ Browses an EOLAB collection based on the user defined start and end date
        as well as the current computational region
    Args:
        collection(string): user defined collection
        bands(list): user defined bands
        mountpoint(string): mountpoint where collection is available
        start_date(datetime): start date as datetime object
        end_date(datetime): end date as datetime object
    Returns:
        scenes_to_import(list): list of scenes to import. Each list item is a
                                dictionary with required information.
                                Dictionary key value pairs are:
                                    - scene(string): scene name
                                    - path(string): complete path to scene dir
                                    - datetime(datetime): timestamp of the scene
                                    - band_paths(dict): Dictionary with
                                                        key-value pairs of
                                                        band filenames and
                                                        corresponding
                                                        semantic label
    """
    scenes_to_import = list()

    collection_dict = eolab_collection_params[collection]
    basepath = os.path.join(mountpoint, collection_dict["basepath"])
    file_format = collection_dict["file_format"]
    clm_dir = collection_dict["CLM_dir"]
    tile_system = collection_dict["tile_system"]
    if tile_system == "MGRS":
        tiles = get_utmcells()
    bands_filesuffixes = {}
    for key, value in collection_dict["bands_filesuffixes"].items():
        if key in bands:
            # we need them swapped later
            bands_filesuffixes[value] = key

    for year in os.listdir(basepath):
        for month in os.listdir(os.path.join(basepath, year)):
            for day in os.listdir(os.path.join(basepath, year, month)):
                folder_date = date(int(year), int(month), int(day))
                if folder_date >= start_date and folder_date < end_date:
                    for scene in os.listdir(os.path.join(basepath,
                                            year, month, day)):

                        if any(tile in scene for tile in tiles):
                            scene_path = os.path.join(basepath, year, month,
                                                      day, scene)
                            scene_dir = {
                                "scene": scene,
                                "path": scene_path,
                                "band_paths": {}
                            }
                            bands_in_scenedir = [item for item in
                                                 os.listdir(scene_path) if
                                                 item.endswith(file_format)]
                            # the rest is S2 MAJA specific:
                            if collection == "S2-L2A-MAJA":
                                # we need the whole date + time, because different
                                # tiles of the same day have different times.
                                # This way, there is no confusion in the STRDS
                                # of which raster map is valid for a given date
                                scenedatetime_str = "-".join(scene.replace(
                                    "-", "_").split("_")[1:3])
                                scene_datetime = datetime.strptime(scenedatetime_str,
                                                                   "%Y%m%d-%H%M%S")
                                scene_dir["datetime"] = scene_datetime
                                for bandsuffix, bandname in bands_filesuffixes.items():
                                    # we need the whole <band>.tif string to rule out
                                    # confusions between B8 and B8A
                                    file_ending = f"{bandsuffix}{file_format}"
                                    if bandname != "S2_CLM":
                                        for file in bands_in_scenedir:
                                            if file_ending in file:
                                                scene_dir["band_paths"][bandname] = file
                                    else:
                                        for file in os.listdir(os.path.join(scene_path,
                                                                            clm_dir)):
                                            if file_ending in file:
                                                scene_dir["band_paths"][bandname] = \
                                                    os.path.join(clm_dir, file)
                            scenes_to_import.append(scene_dir)
    return scenes_to_import


def import_raster(paramdict):
    """ Imports a raster dataset, checks whether a map is empty
        and adds a semantic label
    Args:
        paramdict(dict): Dictionary with import/labelling parameters.
                         Required dictionary key value pairs are:
                         - name(string): name of the imported raster map
                         - input(string): path to the raster file to import
                         - memory(int): allocated memory for the import process
                         - semantic_label(string): semantic label to attach to
                                                   imported raster
                         - scene_datetime(datetime): timestamp of the raster map
    Returns:
        import_dict(dict): Dictionary with information of the imported map.
                           Dictionary key value pairs are:
                           - name(string): name of the imported raster map
                           - datetime(datetime): timestamp of the raster map
                           - map_empty(bool): indicated whether the map consists
                                              of NoData only
                           - band(string): band/semantic label attached to the
                                           imported raster map
    """
    name = paramdict["name"]
    input = paramdict["input"]
    memory = paramdict["memory"]
    # if memory is >= 100000, otherwise GDAL interprets it as bytes, not MB.
    if memory >= 100000:
        memory = memory * 1000000
    semantic_label = paramdict["semantic_label"]
    scene_datetime = paramdict["datetime"]
    grass.message(_(f"Importing {name} ..."))
    # run import, -n flag is required because of r.proj bug, see also
    # https://github.com/OSGeo/grass/issues/2609
    cmd_str = (f"r.import --q input={input} output={name} memory={memory} "
               f"extent=region resample=bilinear resolution=estimated -n")
    cmd = grass.Popen(cmd_str, shell=True, stdout=subprocess.PIPE,
                      stderr=subprocess.PIPE)
    resp = cmd.communicate()
    resp_text = ""
    for resp_line in resp:
        resp_text += resp_line.decode("utf-8")
    empty_strings = [f"<{name}> is empty",
                     "Input raster does not overlap current computational region"]
    if any(string in resp_text for string in empty_strings):
        map_empty = True
        grass.warning(_("Only no-data values found in current region for input "
                        f"raster <{input}>"))
    else:
        map_empty = False
    grass.run_command(
        "r.semantic.label",
        map=name,
        semantic_label=semantic_label,
        operation="add")
    import_dict = {"name": name, "datetime": scene_datetime,
                   "map_empty": map_empty, "band": semantic_label}
    return import_dict


def import_parallel(import_parallel_list, num_processes):
    """ Function that sets up and runs a pool for parallel processing of
        importing. Calls the import_raster function.
    Args:
        import_parallel_list(list): List with dictionaries as items that match
                                    the required input pattern of the
                                    import_raster function
        num_processes(int): Number of parallel processes to use
    Returns:
        output(list): List of dictionaries corresponding to the output format
                      of the import_raster function
    """
    pool = Pool(processes=num_processes)
    output = pool.map(import_raster, import_parallel_list)
    return output


def main():

    global TMPLOC, SRCGISRC, TGTGISRC, GISDBASE
    global rm_rasters, rm_vectors
    global collection_params

    # parameters
    start = options['start']
    end = options['end']
    mountpoint = options["mountpoint"]
    collection = options["collection"]
    bands = options["bands"].split(",")
    output = options["output"]
    archive = options["archive"]

    start_date, end_date = check_start_end(start, end)
    grass.message(_("Filtering available scenes..."))
    if archive == "eolab":
        scenes_to_import = browse_eolab_collection(
            collection=collection,
            bands=bands,
            mountpoint=mountpoint,
            start_date=start_date,
            end_date=end_date)
    if len(scenes_to_import) == 0:
        grass.fatal(_("No scenes matching the spatial and temporal filter "
                      "found. Exiting..."))

    # Importing using the Pool method
    nprocs, used_ram = test_nprocs_memory()
    ram_per_proc = int(used_ram / nprocs)
    grass.message(_("Importing bands..."))
    import_parallel_list = list()
    for scene_dict in scenes_to_import:
        # scene = scene_dict["scene"]
        scene_datetime = scene_dict["datetime"]
        for band, filename in scene_dict["band_paths"].items():
            name_tmp = os.path.splitext(filename)[0].replace("-", "_")
            # there might be something like MASKS/<FILENAME>
            name = name_tmp.split("/")[-1]
            path = os.path.join(scene_dict["path"], filename)
            paramdict = {"input": path,
                         "name": name,
                         "memory": ram_per_proc,
                         "semantic_label": band,
                         "datetime": scene_datetime}
            import_parallel_list.append(paramdict)

    maps_imported = import_parallel(import_parallel_list, nprocs)

    # creating STRDS
    grass.message(_(f"Creating STRDS {output} and registering bands..."))
    grass.run_command(
        "t.create",
        output=output,
        type="strds",
        title=f"{collection}_{output}",
        description=f"{collection}_{output}"
        )
    tmp_file = grass.tempfile()
    with open(tmp_file, "w") as f:
        for item in maps_imported:
            map = item["name"]
            map_datetime = item["datetime"]
            map_empty = item["map_empty"]
            band = item["band"]
            # keep empty rasters only if its the cloud masks
            # this is now S-2 specific, but the if statement can be extended
            # by other cloud bands
            if map_empty is False or band == "S2_CLM":
                str_timestamp = map_datetime.strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{map}|{str_timestamp}\n")
            else:
                rm_rasters.append(map)
    grass.run_command(
        "t.register",
        input=output,
        type="raster",
        file=tmp_file
    )
    os.remove(tmp_file)


if __name__ == "__main__":
    options, flags = grass.parser()
    atexit.register(cleanup)
    main()
