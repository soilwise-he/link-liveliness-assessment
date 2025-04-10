from owslib.wms import WebMapService
from owslib.wmts import WebMapTileService          
from owslib.wfs import WebFeatureService
from owslib.wcs import WebCoverageService
from owslib.ogcapi.features import Features
     
def process_ogc_api(url, ltype, lname, md_id):
    def extract_metadata_urls(urls):
        """Helper to extract metadata URLs"""
        if not urls:
            return []
        metadata_urls = []
        for mu in urls:
            if isinstance(mu, dict) and 'url' in mu:
                metadata_urls.append(mu['url'])
            elif hasattr(mu, 'url'):
                metadata_urls.append(mu.url)
            else:
                metadata_urls.append(str(mu))
        return metadata_urls

    match ltype:
        case 'wms':
            try:
                wms = WebMapService(url, version='1.3.0')
                if lname in list(wms.contents):
                    layer = wms.contents[lname]
                elif len(wms.contents) == 1:
                    layer = wms.contents[next(iter(wms.contents))]
                else:
                    # Search by metadata URL
                    for l in wms.contents.items():
                        if hasattr(l, 'metadataUrls') and l.metadataUrls:
                            urls = extract_metadata_urls(l.metadataUrls)
                            if any(md_id in url for url in urls):
                                layer = l
                                break
                    else:
                        return True  # No match but capabilities successful
                # Convert matched layer to dictionary
                return {
                    'service_type': 'wms',
                    'layer_name': layer.name,
                    'title': layer.title,
                    'abstract': layer.abstract,
                    'keywords': list(layer.keywords) if hasattr(layer, 'keywords') else [],
                    'bbox': layer.boundingBox if hasattr(layer, 'boundingBox') else None,
                    'crs': list(layer.crsOptions) if hasattr(layer, 'crsOptions') else [],
                    'styles': list(layer.styles.keys()) if hasattr(layer, 'styles') else [],
                    'metadata_urls': extract_metadata_urls(layer.metadataUrls) if hasattr(layer, 'metadataUrls') else []
                }
            except Exception as e:
                print(f"Error getting WMS capabilities at {url}: {e}")
                return False

        case 'wmts':
            try:
                wmts = WebMapTileService(url)
                if lname in list(wmts.contents):
                    layer = wmts.contents[lname]
                elif len(wmts.contents) == 1:
                    layer = list(wmts.contents.values())[0]
                else:
                    return True

                return {
                    'service_type': 'wmts',
                    'layer_name': layer.name,
                    'title': layer.title,
                    'abstract': layer.abstract if hasattr(layer, 'abstract') else None,
                    'bbox': layer.boundingBoxWGS84 if hasattr(layer, 'boundingBoxWGS84') else None,
                    'formats': list(layer.formats) if hasattr(layer, 'formats') else [],
                    'tilematrixsets': list(layer.tilematrixsets) if hasattr(layer, 'tilematrixsets') else [],
                    'metadata_urls': extract_metadata_urls(layer.metadataUrls) if hasattr(layer, 'metadataUrls') else []
                }
            except Exception as e:
                print(f"Error getting WMTS capabilities at {url}: {e}")
                return False

        case 'wfs':
            try:
                wfs = WebFeatureService(url=url, version='2.0.0')
                if lname in list(wfs.contents):
                    feature = wfs.contents[lname]
                    schema = wfs.get_schema(lname)
                elif len(wfs.contents) == 1:
                    feature = list(wfs.contents.values())[0]
                    schema = wfs.get_schema(feature.id)
                else:
                    for f in wfs.contents.values():
                        if hasattr(f, 'metadataUrls') and f.metadataUrls:
                            urls = extract_metadata_urls(f.metadataUrls)
                            if any(md_id in url for url in urls):
                                feature = f
                                schema = wfs.get_schema(f.id)
                                break
                    else:
                        return True

                return {
                    'service_type': 'wfs',
                    'feature_name': feature.id,
                    'title': feature.title,
                    'abstract': feature.abstract if hasattr(feature, 'abstract') else None,
                    'keywords': list(feature.keywords) if hasattr(feature, 'keywords') else [],
                    'bbox': feature.boundingBox if hasattr(feature, 'boundingBox') else None,
                    'crs': list(feature.crsOptions) if hasattr(feature, 'crsOptions') else [],
                    'metadata_urls': extract_metadata_urls(feature.metadataUrls) if hasattr(feature, 'metadataUrls') else [],
                    'schema': schema.__dict__ if schema else None
                }
            except Exception as e:
                print(f"Error getting WFS capabilities at {url}: {e}")
                return False

        case 'wcs':
            try:
                wcs = WebCoverageService(url, version='2.0.1')
                if lname in list(wcs.contents):
                    coverage = wcs.contents[lname]
                elif len(wcs.contents) == 1:
                    coverage = list(wcs.contents.values())[0]
                else:
                    for c in wcs.contents.values():
                        if hasattr(c, 'metadataUrls') and c.metadataUrls:
                            urls = extract_metadata_urls(c.metadataUrls)
                            if any(md_id in url for url in urls):
                                coverage = c
                                break
                    else:
                        return True

                return {
                    'service_type': 'wcs',
                    'coverage_name': coverage.id,
                    'title': coverage.title,
                    'abstract': coverage.abstract if hasattr(coverage, 'abstract') else None,
                    'keywords': list(coverage.keywords) if hasattr(coverage, 'keywords') else [],
                    'bbox': coverage.boundingBox if hasattr(coverage, 'boundingBox') else None,
                    'supported_formats': list(coverage.supportedFormats) if hasattr(coverage, 'supportedFormats') else [],
                    'metadata_urls': extract_metadata_urls(coverage.metadataUrls) if hasattr(coverage, 'metadataUrls') else []
                }
            except Exception as e:
                print(f"Error getting WCS capabilities at {url}: {e}")
                return False

        case 'ogcapi':
            try:
                if 'collections/' in url:
                    lname = url.split('collections/').pop().split('/')[0]
                    url = url.split('collections/')[0]
                oaf = Features(url)
               
                if lname not in [None, '']:
                    collection = oaf.collection(lname)
                    return {
                        'service_type': 'ogcapi',
                        'collection_id': collection.id if hasattr(collection, 'id') else None,
                        'title': collection.title if hasattr(collection, 'title') else None,
                        'description': collection.description if hasattr(collection, 'description') else None,
                        'links': [
                            {'href': link.href, 'rel': link.rel, 'type': link.type}
                            for link in collection.links if hasattr(collection, 'links')
                        ],
                        'extent': collection.extent if hasattr(collection, 'extent') else None,
                        'crs': collection.crs if hasattr(collection, 'crs') else None
                    }
                return True
            except Exception as e:
                print(f"Error getting OGC API collection at {url}: {e}")
                return False