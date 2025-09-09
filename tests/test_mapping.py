from connectors.d365.mapping import map_d365_event

sample_raw = {
    "@odata.etag": "W/\"2084511\"",
    "cr83d_due_at": "2025-09-04T05:00:00Z",
    "cr83d_sourcingeventid": "667b9455-c86b-440a-b7f8-438226bb5c2a",
    "cr83d_description": "Done",
    "cr83d_status": "Completed",
    "cr83d_title": "Task 4"
}

def run_demo():
    ev = map_d365_event(sample_raw, tenant_id="demo")
    print("Mapped event object:")
    print(ev.model_dump())

if __name__ == "__main__":
    run_demo()