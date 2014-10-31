CREATE SCHEMA introspect;

SET search_path to introspect;

CREATE TABLE introspect.assignments (
    proposal integer NOT NULL,
    grade integer,
    reviewer integer NOT NULL,
    assigned_by integer,
    assigned_by_name character varying
);


CREATE TABLE introspect.proposals (
    id integer NOT NULL,
    title character varying
);


CREATE TABLE introspect.reviewers (
    reviewer_id integer NOT NULL,
    name character varying NOT NULL,
    manager integer
);


ALTER TABLE ONLY introspect.assignments
    ADD CONSTRAINT assignments_pk PRIMARY KEY (proposal, reviewer);


ALTER TABLE ONLY introspect.proposals
    ADD CONSTRAINT proposals_pkey PRIMARY KEY (id);


ALTER TABLE ONLY introspect.reviewers
    ADD CONSTRAINT reviewer_pk PRIMARY KEY (reviewer_id);


ALTER TABLE ONLY introspect.reviewers
    ADD CONSTRAINT reviewer_unique UNIQUE (name, reviewer_id);


ALTER TABLE ONLY introspect.assignments
    ADD CONSTRAINT proposals_fk FOREIGN KEY (proposal) REFERENCES introspect.proposals(id);


ALTER TABLE ONLY introspect.reviewers
    ADD CONSTRAINT reviewer_fk FOREIGN KEY (manager) REFERENCES introspect.reviewers(reviewer_id);


ALTER TABLE ONLY introspect.assignments
    ADD CONSTRAINT reviewer_fk FOREIGN KEY (reviewer) REFERENCES introspect.reviewers(reviewer_id);

ALTER TABLE ONLY assignments
    ADD CONSTRAINT reviewer_fk2 FOREIGN KEY (assigned_by, assigned_by_name) REFERENCES reviewers(reviewer_id, name);

