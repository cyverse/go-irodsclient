package types

// DataType is a type for data type
type DataType string

const (
	GENERIC_DT                      DataType = "generic"
	TEXT_DT                         DataType = "text"
	ASCII_TEXT_DT                   DataType = "ascii text"                   // *.txt
	ASCII_COMPRESSED_LEMPEL_ZIV_DT  DataType = "ascii compressed Lempel-Ziv"  // *.z, *.zip, *.gz
	ASCII_COMPRESSED_HUFFMAN_DT     DataType = "ascii compressed Huffman"     // *.z, *.zip, *.gz
	EBCDIC_TEXT_DT                  DataType = "ebcdic text"                  // *.txt
	EBCDIC_COMPRESSED_LEMPEL_ZIV_DT DataType = "ebcdic compressed Lempel-Ziv" // *.z, *.zip, *.gz
	EBCDIC_COMPRESSED_HUFFMAN_DT    DataType = "ebcdic compressed Huffman"    // *.z, *.zip, *.gz
	IMAGE_DT                        DataType = "image"
	TIFF_IMAGE_DT                   DataType = "tiff image"     // *.tif, *.tiff
	UUENCODED_TIFF_DT               DataType = "uuencoded tiff" // *.uu
	GIF_IMAGE_DT                    DataType = "gif image"      // *.gif
	JPEG_IMAGE_DT                   DataType = "jpeg image"     // *.jpeg, *.jpg
	PBM_IMAGE_DT                    DataType = "pbm image"      // *.pbm
	FIG_IMAGE_DT                    DataType = "fig image"      // *.fig
	FITS_IMAGE_DT                   DataType = "FITS image"     // *.fits, *.fit
	DICOM_IMAGE_DT                  DataType = "DICOM image"    // *.IMA, *.ima
	PRINT_FORMAT_DT                 DataType = "print-format"
	LATEX_FORMAT_DT                 DataType = "LaTeX format"      // *.tex
	TROFF_FORMAT_DT                 DataType = "Troff format"      // *.trf, *.trof
	POSTSCRIPT_FORMAT_DT            DataType = "Postscript format" // *.ps
	DVI_FORMAT_DT                   DataType = "DVI format"        // *.dvi
	WORD_FORMAT_DT                  DataType = "Word format"       // *.doc, *.rtf
	PROGRAM_CODE_DT                 DataType = "program code"
	SQL_SCRIPT_DT                   DataType = "SQL script"          // *.sql
	C_CODE_DT                       DataType = "C code"              // *.c
	C_INCLUDE_FILE_DT               DataType = "C include file"      // *.c
	FORTRAN_CODE_DT                 DataType = "fortran code"        // *.f
	OBJECT_CODE_DT                  DataType = "object code"         // *.o
	LIBRARY_CODE_DT                 DataType = "library code"        // *.a
	DATA_FILE_DT                    DataType = "data file"           // *.dat
	HTML_FILE_DT                    DataType = "html"                // *.htm, *.html
	SGML_FILE_DT                    DataType = "SGML File"           // *.sgm, *.sgml
	WAVE_AUDIO_DT                   DataType = "Wave Audio"          // *.wav
	TAR_FILE_DT                     DataType = "tar file"            // *.tar
	COMPRESSED_TAR_FILE_DT          DataType = "compressed tar file" // *.tz, *.tgz, *.zip
	JAVA_CODE_DT                    DataType = "java code"           // *.jav, *.java
	PERL_SCRIPT_DT                  DataType = "perl script"         // *.pl
	TCL_SCRIPT_DT                   DataType = "tcl script"          // *.tcl
	LINK_CODE_DT                    DataType = "link code"           // *.o
	SHADOW_OBJECT_DT                DataType = "shadow object"
	DATABASE_SHADOW_OBJECT_DT       DataType = "database shadow object"
	DIRECTORY_SHADOW_OBJECT_DT      DataType = "directory shadow object"
	DATABASE_DT                     DataType = "database"
	STREAMS_DT                      DataType = "streams"
	AUDIO_STREAMS_DT                DataType = "audio streams"
	REAL_AUDIO_DT                   DataType = "realAudio" // *.ra
	VIDEO_STREAMS_DT                DataType = "video streams"
	REAL_VIDEO_DT                   DataType = "realVideo"                     // *.rv
	MPEG_DT                         DataType = "MPEG"                          // *.mpeg, *.mpg
	AVI_DT                          DataType = "AVI"                           // *.avi
	PNG_DT                          DataType = "PNG-Portable Network Graphics" // *.png
	MP3_DT                          DataType = "MP3 - MPEG Audio"              // *.mp3, *.mpa
	WMV_DT                          DataType = "WMV-Windows Media Video"       // *.wmv
	BMP_DT                          DataType = "BMP -Bit Map"                  // *.bmp
	CSS_DT                          DataType = "CSS-Cascading Style Sheet"
	XML_DT                          DataType = "xml" // *.xml
	SLIDE_DT                        DataType = "Slide"
	POWER_POINT_SLIDE_DT            DataType = "Power Point Slide" // *.ppt
	SPREAD_SHEET_DT                 DataType = "Spread Sheet"
	EXCEL_SPREAD_SHEET_DT           DataType = "Excel Spread Sheet" // *.xls
	DOCUMENT_DT                     DataType = "Document"
	MSWORD_DOCUMENT_DT              DataType = "MSWord Document" // *.doc, *.rtf
	PDF_DOCUMENT_DT                 DataType = "PDF Document"    // *.pdf
	EXECUTABLE_DT                   DataType = "Executable"
	NT_EXECUTABLE_DT                DataType = "NT Executable"
	SOLARIS_EXECUTABLE_DT           DataType = "Solaris Executable"
	AIX_EXECUTABLE_DT               DataType = "AIX Executable"
	MAC_EXECUTABLE_DT               DataType = "Mac Executable"
	MAC_OSX_EXECURABLE_DT           DataType = "Mac OSX Executable"
	CRAY_EXECUTABLE_DT              DataType = "Cray Executable"
	SGI_EXECUTABLE_DT               DataType = "SGI Executable"
	DLL_DT                          DataType = "DLL"
	NT_DLL_DT                       DataType = "NT DLL"
	SOLARIS_DLL_DT                  DataType = "Solaris DLL"
	AIX_DLL_DT                      DataType = "AIX DLL"
	MAC_DLL_DT                      DataType = "Mac DLL"
	CRAY_DLL_DT                     DataType = "Cray DLL"
	SGI_DLL_DT                      DataType = "SGI DLL"
	MOVIE_DT                        DataType = "Movie"
	MPEG_MOVIE_DT                   DataType = "MPEG Movie"      // *.mpeg, *.mpg
	MPEG3_MOVIE_DT                  DataType = "MPEG 3 Movie"    // *.mpeg, *.mpg
	QUICKTIME_MOVIE_DT              DataType = "Quicktime Movie" // *.mov
	COMPRESSED_FILE_DT              DataType = "compressed file"
	COMPRESSED_MMCIF_FILE_DT        DataType = "compressed mmCIF file" // *.cif, *.mmcif
	COMPRESSED_PDB_FILE_DT          DataType = "compressed PDB file"   // *.pdb
	BINARY_FILE_DT                  DataType = "binary file"
	URL_DT                          DataType = "URL" // *.htm, *.html
	NSF_AWARD_ABSTRACTS_DT          DataType = "NSF Award Abstracts"
	EMAIL_DT                        DataType = "email"
	ORB_DATA_DT                     DataType = "orb data"
	DATASCOPE_DATA_DT               DataType = "datascope data"
	DICOM_HEADER_DT                 DataType = "DICOM header"
	XML_SCHEMA_DT                   DataType = "XML Schema" // *.xsd
	TAR_BUNDLE_DT                   DataType = "tar bundle"
	DATABASE_OBJECT_DT              DataType = "database object"
	MSO_DT                          DataType = "mso"
	GZIP_TAR_DT                     DataType = "gzipTar"   // *.tar.gz
	BZIP2_TAR_DT                    DataType = "bzip2Tar"  // *.tar.bz2
	GZIP_FILE_DT                    DataType = "gzipFile"  // *.gz
	BZIP2_FILE_DT                   DataType = "bzip2File" // *.bz2
	ZIP_FILE_DT                     DataType = "zipFile"   // *.zip
	GZIP_TAR_BUNDLE_DT              DataType = "gzipTar bundle"
	BZIP2_TAR_BUNDLE_DT             DataType = "bzip2Tar bundle"
	ZIP_FILE_BUNDLE_DT              DataType = "zipFile bundle"
	MSSO_FILE_DT                    DataType = "msso file"
)
