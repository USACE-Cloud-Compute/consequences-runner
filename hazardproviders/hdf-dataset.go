package hazardproviders

import (
	"errors"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/usace/go-hdf5"
)

func OpenFile(filepath string) (*hdf5.File, error) {

	var f *hdf5.File
	var err error

	/*	url := strings.ToLower(filepath)
		if strings.HasPrefix(url, "https") {

			fapl_id, _ := hdf5.NewPropList(hdf5.P_FILE_ACCESS)

			ros3_fa := hdf5.H5FD_ROS3_FAPL{
				Version:               1,
				Authenticate:          true,
				AWS_REGION:            os.Getenv("AWS_REGION"),
				AWS_ACCESS_KEY_ID:     os.Getenv("AWS_ACCESS_KEY_ID"),
				AWS_SECRET_ACCESS_KEY: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			}

			hdf5.H5PsetFaplRos3d(fapl_id, ros3_fa)
			f, err = hdf5.OpenFileWithProp(url, hdf5.F_ACC_RDONLY, fapl_id)
		} else { */
	f, err = hdf5.OpenFile(filepath, hdf5.F_ACC_RDONLY)
	//}
	return f, err

}

func NewHdfStrSet(sizes ...int) HdfStrSet {
	result := 0
	for _, v := range sizes {
		result += v
	}
	return HdfStrSet{sizes, result}
}

type HdfStrSet struct {
	sizes      []int
	buffersize int
}

func (h HdfStrSet) ToString() string {
	s := make([]string, len(h.sizes))
	for i, v := range h.sizes {
		s[i] = strconv.Itoa(v)
	}
	return strings.Join(s, ",")
}

func (h *HdfStrSet) FromString(s string) error {
	sa := strings.Split(s, ",")
	sizes := make([]int, len(sa))
	for i, v := range sa {
		size, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		sizes[i] = size
	}
	h.sizes = sizes
	return nil
}

func (h *HdfStrSet) Cols() []int {
	return h.sizes
}

func (h *HdfStrSet) RowSize() int {
	return h.buffersize
}

// offset in bytes to the column requested
func (h HdfStrSet) BytesTo(col int) (int, error) {
	result := 0
	if col > len(h.sizes)-1 {
		return -1, errors.New("Column number requested exceeds the size of the string array")
	}
	for i := 0; i < col-1; i++ {
		result += h.sizes[i]
	}
	return result, nil
}

func (h HdfStrSet) Size(col int) (int, error) {
	if col > len(h.sizes)-1 {
		return -1, errors.New("Column number requested exceeds the size of the string array")
	}
	return h.sizes[col], nil
}

///////////////////////////////////////////////////////
///////////HDF Interfaces///////////////

type HdfReader interface {
	Dims() []uint
	Read() (*HdfData, error)
	ReadInto(dest interface{}) error
	ReadSubset(rowrange []int, colrange []int) (*HdfData, error)
	ReadSubsetInto(dest interface{}, rowrange []int, colrange []int) error
	Close()
}

type HdfReadOptions struct {
	Dtype              reflect.Kind
	Strsizes           HdfStrSet
	IncrementalRead    bool
	IncrementalReadDir int //0 by row, 1 by column
	IncrementSize      int
	ReadOnCreate       bool //reads data in when the new datraset is created
	Async              bool
}

type HdfData struct {
	DsetDims []uint //dimension of the full dataset
	Dims     []uint //dimension of the extracted dataset
	Buffer   interface{}
}

/////////////////////HDF Dataset//////////////////////

type readincrement struct {
	start int
	end   int //range of rows/colums currently in memory
}

type HdfDataset struct {
	Data      *HdfData
	Reader    HdfReader
	options   *HdfReadOptions
	dsetdims  []uint
	increment readincrement
}

func NewHdfDataset(filepath string, datapath string, options HdfReadOptions) (*HdfDataset, error) {
	var data *HdfData
	var err error
	var ri readincrement = readincrement{0, -1}
	var reader HdfReader

	reader, err = NewHdfReaderSync(filepath, datapath, options)
	if err != nil {
		return nil, err
	}

	if options.ReadOnCreate {
		if !options.IncrementalRead {
			data, err = reader.Read()
			if err != nil {
				return nil, err
			}
		}
	}

	dims := reader.Dims()

	return &HdfDataset{
		Reader:    reader,
		Data:      data,
		options:   &options,
		dsetdims:  dims,
		increment: ri,
	}, nil
}

func (h *HdfDataset) Close() {
	if h.Reader != nil {
		h.Reader.Close()
	}
}

func (h *HdfDataset) Dims() []uint {
	return h.dsetdims
}

func (h *HdfDataset) Read() error {
	data, err := h.Reader.Read()
	if err != nil {
		return err
	}
	h.Data = data
	h.dsetdims = data.Dims
	return nil
}

func (h *HdfDataset) ReadSubset(rowrange []int, colrange []int) error {
	data, err := h.Reader.ReadSubset(rowrange, colrange)
	if err != nil {
		return err
	}
	h.Data = data
	h.dsetdims = data.Dims
	return nil
}

func (h *HdfDataset) ReadInto(dest interface{}) error {
	return h.Reader.ReadInto(dest)
}

func (h *HdfDataset) ReadSubsetInto(dest interface{}, rowrange []int, colrange []int) error {
	return h.Reader.ReadSubsetInto(dest, rowrange, colrange)
}

func (h *HdfDataset) Rows() int {
	return int(h.dsetdims[0])
}

func (h *HdfDataset) Cols() int {
	if len(h.dsetdims) > 1 {
		return int(h.dsetdims[1])
	} else {
		return 1
	}
}

func (h *HdfDataset) ReadRow(r int, dest interface{}) error {
	if h.options.IncrementalRead {
		if h.options.IncrementalReadDir != 0 {
			return errors.New("Cannot perform row read on dataset configured for incremental readng of columns")
		}
		return h.readIncrementRow(r, dest)
	} else {
		if h.options.Dtype == reflect.String {
			h.readVectorString(true, r, dest) //@TODO need to handle errors better here.  It looks like all of the calls will panic on error, so nothing is being returned.
			return nil
		} else {
			h.readVector(true, r, dest) //@TODO need to handle errors better here.  It looks like all of the calls will panic on error, so nothing is being returned.
			return nil
		}
	}
}

func (h *HdfDataset) readIncrementRow(r int, dest interface{}) error {
	if r > h.increment.end {
		rowend := r + h.options.IncrementSize - 1
		if rowend+1 >= int(h.dsetdims[0]) {
			rowend = int(h.dsetdims[0]) - 1
		}
		data, err := h.Reader.ReadSubset([]int{r, rowend}, []int{0, int(h.dsetdims[1]) - 1})
		if err != nil {
			return err
		}
		h.Data = data
		h.increment = readincrement{r, rowend}
	}
	virtualRow := r - h.increment.start
	h.readVector(true, virtualRow, dest)
	return nil
}

func (h *HdfDataset) ReadColumn(c int, dest interface{}) error {
	if h.options.IncrementalRead {
		if h.options.IncrementalReadDir != 1 {
			return errors.New("Cannot perform column read on dataset configured for incremental readng of rows")
		}
		return h.readIncrementColumn(c, dest)
	} else {
		h.readVector(false, c, dest) //@TODO need to handle errors better here.  It looks like all of the calls will panic on error, so nothing is being returned.
		return nil
	}

}

func (h *HdfDataset) readIncrementColumn(c int, dest interface{}) error {
	if c > h.increment.end {
		colend := c + h.options.IncrementSize - 1
		if colend+1 >= int(h.dsetdims[1]) {
			colend = int(h.dsetdims[1]) - 1
		}
		data, err := h.Reader.ReadSubset([]int{0, int(h.dsetdims[0]) - 1}, []int{c, colend})
		if err != nil {
			return err
		}
		h.Data = data
		h.increment = readincrement{c, colend}
	}
	virtualCol := c - h.increment.start
	h.readVector(false, virtualCol, dest)
	return nil
}

func (h *HdfDataset) readVector(rowread bool, index int, dest interface{}) {
	colnum := h.Cols()
	dv := reflect.ValueOf(h.Data.Buffer)
	if dv.Kind() == reflect.Ptr {
		dv = dv.Elem()
	}
	value := reflect.Indirect(reflect.ValueOf(dest))
	if rowread {
		offset := index * colnum
		value.Set(dv.Slice(offset, offset+colnum))
	} else {
		if h.options.IncrementalRead {
			colnum = h.increment.end - h.increment.start + 1
		}
		value.Set(value.Slice(0, 0)) //truncate the column slice since we will be using append to add new values
		for i := 0; i < h.Rows(); i++ {
			offset := i*colnum + index
			value.Set(reflect.Append(value, dv.Index(offset)))
		}
	}
}

func (h *HdfDataset) readVectorString(rowread bool, index int, dest interface{}) {
	buffer := *h.Data.Buffer.(*[]uint8)
	value := reflect.Indirect(reflect.ValueOf(dest))
	if rowread {
		offset := index * h.options.Strsizes.RowSize()
		for _, v := range h.options.Strsizes.Cols() {
			value.Set(reflect.Append(value, reflect.ValueOf(strings.TrimSpace(string(buffer[offset:offset+v]))))) //@TODO might need to revisit this and strip null values (\x00)
			offset += v
		}
	} else {
		value.Set(value.Slice(0, 0)) //truncate the column slice since we will be using append to add new values
		for i := 0; i < h.Rows(); i++ {
			bt, _ := h.options.Strsizes.BytesTo(index)
			sz, _ := h.options.Strsizes.Size(index)
			offset := i*h.options.Strsizes.RowSize() + bt
			value.Set(reflect.Append(value, reflect.ValueOf(strings.TrimSpace(string(buffer[offset:offset+sz])))))
		}
	}
}

///////////////////////////////////////////////////////
/////////////////////HDF Reader Sync//////////////////////

type HdfReaderSync struct {
	file     *hdf5.File
	dset     *hdf5.Dataset
	dtype    reflect.Kind
	dims     []uint //dimension of the source dataset
	strsizes HdfStrSet
}

func NewHdfReaderSync(filepath string, datapath string, options HdfReadOptions) (HdfReader, error) {
	f, err := OpenFile(filepath)
	if err != nil {
		log.Fatal(err)
	}
	dset, err := f.OpenDataset(datapath)
	if err != nil {
		return nil, err
	}
	space := dset.Space()
	defer space.Close()
	dims, max, err := space.SimpleExtentDims()
	log.Printf("Dataset Diminsions are %v, %v\n", dims, max)
	if err != nil {
		return nil, err
	}

	return &HdfReaderSync{
		file:     f,
		dset:     dset,
		dims:     dims,
		dtype:    options.Dtype,
		strsizes: options.Strsizes,
	}, nil
}

func (h *HdfReaderSync) Close() {
	defer h.dset.Close()
	defer h.file.Close()
}

func (h *HdfReaderSync) Dims() []uint {
	return h.dims
}
func (h *HdfReaderSync) Read() (*HdfData, error) {
	dest, err := makeDataset(h.dims, h.dtype, h.strsizes.RowSize())
	if err != nil {
		return nil, err
	}
	err = h.dset.Read(dest)
	if err != nil {
		return nil, err
	}
	data := HdfData{
		DsetDims: h.dims,
		Dims:     h.dims,
		Buffer:   dest,
	}
	return &data, nil
}

func (h *HdfReaderSync) ReadInto(dest interface{}) error {
	err := h.dset.Read(dest)
	if err != nil {
		return err
	}
	return nil
}

func (h *HdfReaderSync) ReadSubset(rowrange []int, colrange []int) (*HdfData, error) {
	filespace := h.dset.Space()
	defer filespace.Close()

	memspace, dims, err := h.getMemspace(filespace, rowrange, colrange)
	if err != nil {
		return nil, err
	}
	defer memspace.Close()

	dest, err := makeDataset(dims, h.dtype, h.strsizes.RowSize())
	if err != nil {
		return nil, err
	}

	err = h.dset.ReadSubset(dest, memspace, filespace)
	if err != nil {
		return nil, err
	}
	return &HdfData{
		DsetDims: h.dims,
		Dims:     dims,
		Buffer:   dest}, nil
}

func (h *HdfReaderSync) ReadSubsetInto(dest interface{}, rowrange []int, colrange []int) error {
	filespace := h.dset.Space()
	defer filespace.Close()

	memspace, _, err := h.getMemspace(filespace, rowrange, colrange)
	if err != nil {
		return err
	}
	defer memspace.Close()

	err = h.dset.ReadSubset(dest, memspace, filespace)
	if err != nil {
		return err
	}
	return nil
}

func (h *HdfReaderSync) getMemspace(filespace *hdf5.Dataspace, rowrange []int, colrange []int) (*hdf5.Dataspace, []uint, error) {
	rcount := uint(rowrange[1]-rowrange[0]) + 1
	colcount := uint(colrange[1]-colrange[0]) + 1
	offset, stride, count, block := []uint{uint(rowrange[0]), uint(colrange[0])}, []uint{1, 1}, []uint{rcount, colcount}, []uint{1, 1}

	err := filespace.SelectHyperslab(offset[:], stride[:], count[:], block[:])
	if err != nil {
		return nil, []uint{}, err
	}

	dims, maxdims := []uint{rcount, colcount}, []uint{rcount, colcount}
	if err != nil {
		return nil, []uint{}, err
	}
	memspace, err := hdf5.CreateSimpleDataspace(dims[:], maxdims[:])
	if err != nil {
		return nil, []uint{}, err
	}
	return memspace, dims, nil
}

func makeDataset(dims []uint, dtype reflect.Kind, strbuffersize int) (interface{}, error) {
	switch dtype {
	case reflect.Float32:
		d := make([]float32, arraySize(dims))
		return &d, nil
	case reflect.Float64:
		d := make([]float64, arraySize(dims))
		return &d, nil
	case reflect.Int32:
		d := make([]int32, arraySize(dims))
		return &d, nil
	case reflect.String:
		d := make([]byte, strbuffersize*int(dims[0]))
		return &d, nil
	default:
		return nil, errors.New("Unable to make dataset: invalid datatype")
	}
}

func arraySize(dims []uint) uint {
	var size uint = 1
	for i := 0; i < len(dims); i++ {
		size = size * dims[i]
	}
	return size
}
