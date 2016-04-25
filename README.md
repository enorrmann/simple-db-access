# simple-db-access
# usos

    @Inject
    private SimpleDbAccess db;

    @Override
    public SimpleDto create(SimpleDto simpleDto) {
        db.insert(simpleDto, "BL_TEMA");
        return simpleDto;
    }

    @Override
    public SimpleDto update(String id, SimpleDto simpleDto) {
        SimpleDto response = db.update(simpleDto,"BL_TEMA");
        return response;
    }

    @Override
    public List<SimpleDto> all() {
        return db.select(null, "BL_TEMA");
    }

    @Override
    public SimpleDto findById(String id) {
        return db.findById(id, "BL_TEMA");
    }
