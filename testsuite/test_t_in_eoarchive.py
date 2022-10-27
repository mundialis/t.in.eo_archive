"""
Name:       t.in.eoarchive test
Purpose:    Tests t.in.eoarchive GRASS module.
Author:     Guido Riembauer
Copyright:  (C) 2022 mundialis GmbH & Co. KG, and the GRASS
            Development Team
Licence:    This program is free software; you can redistribute it and/or modify
            it under the terms of the GNU General Public License as published by
            the Free Software Foundation; either version 3 of the License, or
            (at your option) any later version.

            This program is distributed in the hope that it will be useful,
            but WITHOUT ANY WARRANTY; without even the implied warranty of
            MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
            GNU General Public License for more details.
"""
import os

from grass.gunittest.case import TestCase
from grass.gunittest.main import test
from grass.gunittest.gmodules import SimpleModule
import grass.script as grass
import grass.temporal as tgis


# Run in EPSG:4326 location

class TestTInEoarchive(TestCase):
    pid_str = str(os.getpid())
    reg_bonn_n = 50.8209
    reg_bonn_s = 50.6170
    reg_bonn_w = 6.9955
    reg_bonn_e = 7.2061

    reg_madrid_n = 40.48
    reg_madrid_s = 40.3
    reg_madrid_w = -3.83
    reg_madrid_e = -3.58

    old_region = f"old_region_{pid_str}"

    gisenv = grass.gisenv()
    mapset = gisenv['MAPSET']
    output_strds = f"test_strds_{pid_str}"
    output_strds_mapset = f"{output_strds}@{mapset}"

    @classmethod
    def setUpClass(self):
        tgis.init()
        grass.run_command("g.region", save=self.old_region)

    @classmethod
    def tearDownClass(self):
        """Remove the temporary region"""
        grass.run_command("g.remove", type="region", name=self.old_region,
                          flags="f")

    def tearDown(self):
        """Remove the outputs created
        This is executed after each test run.
        """
        grass.run_command("g.region", region=self.old_region)
        if self.checkSTRDSexists(self.output_strds_mapset) is True:
            grass.run_command("t.remove", inputs=self.output_strds, flags="fd")

    def checkSTRDSexists(self, strds_id):
        dbif = tgis.SQLDatabaseInterfaceConnection()
        dbif.connect()
        strds = tgis.SpaceTimeRasterDataset(strds_id)
        strds.select(dbif=dbif)
        dbif.close()
        exists = strds.is_in_db()
        return exists

    def getSTRDSrasters(self, strds_id):
        dbif = tgis.SQLDatabaseInterfaceConnection()
        dbif.connect()
        strds = tgis.SpaceTimeRasterDataset(strds_id)
        strds.select(dbif=dbif)
        maps = strds.get_registered_maps_as_objects(dbif=dbif)
        dbif.close()
        map_list = list()
        for map in maps:
            map_dict = {"map": map.get_name(),
                        "start_time": map.get_temporal_extent_as_tuple()[0],
                        "semantic_label": map.metadata.get_semantic_label()
                        }
            map_list.append(map_dict)
        return map_list

    def test_sentinel2_import_success_july(self):
        """ Test a successful STRDS import for July 2022 data """
        grass.run_command("g.region", n=self.reg_bonn_n, s=self.reg_bonn_s,
                          w=self.reg_bonn_w, e=self.reg_bonn_e, cols=1, rows=1)
        s2_july = SimpleModule(
            "t.in.eoarchive",
            start="2022-07-01",
            end="2022-07-15",
            bands=["S2_B4,S2_B8,S2_CLM"],
            archive="eolab",
            collection="S2-L2A-MAJA",
            mountpoint="/codede",
            output=self.output_strds
        )
        self.assertModule(s2_july)
        # assert that STRDS exists
        strds_exists = self.checkSTRDSexists(self.output_strds_mapset)
        self.assertTrue(strds_exists, f"Creation of STRDS {self.output_strds} failed")
        strds_rasters = self.getSTRDSrasters(self.output_strds_mapset)
        num_rasters = len(strds_rasters)
        self.assertEqual(num_rasters, 16, (f"Resulting STRDS has {num_rasters}"
                         ", should be 16"))
        # check that the raster exists and that it is not NULL only
        # (unless it is a cloud mask)
        for raster in strds_rasters:
            map = raster["map"]
            self.assertRasterExists(map, f"Raster {map} not found")
            if raster["semantic_label"] != "S2_CLM":
                grass.run_command("g.region", raster=map)
                univar = grass.parse_command("r.univar", map=map, flags="g")
                self.assertNotEqual(univar["null_cells"], univar["cells"],
                                    f"Raster map {map} is NoData only")

    def test_sentinel2_import_fail_july(self):
        """ Test a failed STRDS import for July 2022 data
            (region outside available data)
        """
        grass.run_command("g.region", n=self.reg_madrid_n, s=self.reg_madrid_s,
                          w=self.reg_madrid_w, e=self.reg_madrid_e, cols=1, rows=1)
        s2_july = SimpleModule(
            "t.in.eoarchive",
            start="2022-07-01",
            end="2022-07-15",
            bands=["S2_B4,S2_B8,S2_CLM"],
            archive="eolab",
            collection="S2-L2A-MAJA",
            mountpoint="/codede",
            output=self.output_strds
        )
        self.assertModuleFail(s2_july)
        stderr = s2_july.outputs.stderr
        check_str = "No scenes matching the spatial and temporal filter found."

        self.assertIn(check_str, stderr, ("Wrong error message, it should be:\n"
                                          f"{check_str}"))


if __name__ == "__main__":
    test()
