var activeRulesCacheArray = [];
var selectedRuleContextObject = null;

//  CHANNELS DATA ARRAY:
var parameterOptionsMappingLabels = [
    { value: "aod", text: "Account Opening Days" },
    { value: "narration", text: "Narration" },
    { value: "drcr", text: "Dr/Cr" },
    { value: "amount", text: "Transaction Amount" },
    { value: "cum_credit", text: "Cumulative Credit Amount" },
    { value: "cum_debit", text: "Cumulative Debit Amount" },
    { value: "channel", text: "Channel" }
];

async function fetchSystemRulesFromDatabaseQueue() {
    try {
        var res = await fetch('/api/get-rules');
        activeRulesCacheArray = await res.json();
        renderRulesLeftSidebarItems();
    } catch(e) { console.error(e); }
}

function renderRulesLeftSidebarItems() {
    var container = document.getElementById('ruleItemsListingContainer');
    container.innerHTML = '';
    if (activeRulesCacheArray.length === 0) {
        container.innerHTML = '<div style="text-align:center; padding:15px; color:#9ca3af; font-size:13px; font-style:italic;">No dynamic rules configured.</div>';
        return;
    }
    activeRulesCacheArray.forEach(function(rule) {
        var itemRow = document.createElement('div');
        itemRow.className = 'rule-item-row';
        if (selectedRuleContextObject && selectedRuleContextObject.id === rule.id) { itemRow.className += ' active'; }
        itemRow.setAttribute('onclick', 'displaySelectedRuleDetailsPerspective(' + rule.id + ')');
        
        var nameLbl = document.createElement('div');
        nameLbl.className = 'rule-name-lbl';
        nameLbl.innerText = rule.name;
        
        var deleteBtn = document.createElement('button');
        deleteBtn.className = 'rule-delete-trigger-btn';
        deleteBtn.innerText = 'Delete';
        deleteBtn.setAttribute('onclick', 'executeTargetRuleDeletionPipeline(event, ' + rule.id + ')');
        
        itemRow.appendChild(nameLbl);
        itemRow.appendChild(deleteBtn);
        container.appendChild(itemRow);
    });
}

function displaySelectedRuleDetailsPerspective(ruleId) {
    var targetRule = activeRulesCacheArray.find(function(r) { return r.id === ruleId; });
    if (!targetRule) return;
    selectedRuleContextObject = targetRule;
    renderRulesLeftSidebarItems();
    
    var canvas = document.getElementById('rightWorkspaceWorkspaceCanvas');
    var conditionsHtml = '';
    
    if (targetRule.conditions && targetRule.conditions.length > 0) {
        targetRule.conditions.forEach(function(cond) {
            var matchLabel = parameterOptionsMappingLabels.find(function(o) { return o.value === cond.parameter; });
            var textDisplayParam = matchLabel ? matchLabel.text : cond.parameter;
            conditionsHtml += '<span class="readonly-condition-badge"><b>' + textDisplayParam + '</b> ' + cond.operator + ' "' + cond.value + '"</span>';
        });
    } else {
        conditionsHtml = '<span style="color:#9ca3af; font-style:italic; font-size:14px;">No configuration clauses mapped.</span>';
    }

    canvas.innerHTML = 
        '<div class="form-section-title">Configure Rule Matrix</div>' +
        '<div class="form-row-split">' +
            '<div class="input-group-row"><label>Rule Name</label><input type="text" class="text-input-field" value="' + targetRule.name + '" readonly></div>' +
            '<div class="input-group-row"><label>Rule Description / Objective</label><input type="text" class="text-input-field" value="' + (targetRule.description || '') + '" readonly></div>' +
        '</div>' +
        '<div class="input-group-row"><label class="section-subtitle-lbl">Condition Rows (Active Matrix Evaluation View)</label><div style="margin-top:5px;">' + conditionsHtml + '</div></div>';
}

function openNewRuleCreationFormPerspective() {
    selectedRuleContextObject = null;
    renderRulesLeftSidebarItems();
    
    var canvas = document.getElementById('rightWorkspaceWorkspaceCanvas');
    canvas.innerHTML = 
        '<div class="form-section-title">Configure Rule Matrix</div>' +
        '<div class="form-row-split">' +
            '<div class="input-group-row"><label>Rule Name</label><input type="text" id="newRuleNameInputField" class="text-input-field" placeholder="Enter rule name"></div>' +
            '<div class="input-group-row"><label>Rule Description / Objective</label><input type="text" id="newRuleDescInputField" class="text-input-field" placeholder="Enter objective summary"></div>' +
        '</div>' +
        '<div class="input-group-row" style="margin-top:5px;"><label class="section-subtitle-lbl">Condition Rows (Live Configuration Grid)</label>' +
        '<div class="condition-matrix-box" id="dynamicFormConditionsWrapperBox"></div>' +
        '<button class="add-condition-clause-link" onclick="appendFreshConditionInputRowToForm()">+ Add Condition Row</button></div>' +
        '<div class="action-dispatch-panel">' +
        '<button class="save-rule-action-btn" onclick="commitCompiledFormRuleToDatabase()">Save Rule Configuration</button>' +
        '</div>';
        
    appendFreshConditionInputRowToForm();
}

function appendFreshConditionInputRowToForm() {
    var box = document.getElementById('dynamicFormConditionsWrapperBox');
    var row = document.createElement('div');
    row.className = 'condition-row-item';
    
    var paramSelectHtml = '<select class="select-dropdown-field data-param-selector">';
    parameterOptionsMappingLabels.forEach(function(opt) {
        paramSelectHtml += '<option value="' + opt.value + '">' + opt.text + '</option>';
    });
    paramSelectHtml += '</select>';

    row.innerHTML = paramSelectHtml +
        '<select class="operator-dropdown-field data-operator-selector"><option value="<">&lt;</option><option value=">">&gt;</option><option value="=">=</option><option value="!=">!=</option></select>' +
        '<input type="text" class="value-input-box data-value-input" placeholder="Value...">' +
        '<select class="logic-link-dropdown-field"><option value="AND">AND</option><option value="OR">OR</option></select>' +
        '<button class="remove-condition-row-btn" onclick="this.parentElement.remove()">×</button>';
    box.appendChild(row);
}

async function executeTargetRuleDeletionPipeline(event, ruleId) {
    if (event) { event.stopPropagation(); }
    if (!confirm("Are you completely sure you want to delete this rule?")) return;
    try {
        var res = await fetch('/api/delete-rule/' + ruleId, { method: 'POST' });
        var statusResult = await res.json();
        if (statusResult.status === 'success') {
            if (selectedRuleContextObject && selectedRuleContextObject.id === ruleId) {
                selectedRuleContextObject = null;
                document.getElementById('rightWorkspaceWorkspaceCanvas').innerHTML = '<div class="empty-state-notice">Select or Add a Rule</div>';
            }
            await fetchSystemRulesFromDatabaseQueue();
        }
    } catch(e) { console.error(e); }
}

async function commitCompiledFormRuleToDatabase() {
    var name = document.getElementById('newRuleNameInputField').value.trim();
    var desc = document.getElementById('newRuleDescInputField').value.trim();
    if (!name) { alert("Please provide a valid rule name."); return; }
    
    var rows = document.querySelectorAll('#dynamicFormConditionsWrapperBox .condition-row-item');
    var conditions = [];
    rows.forEach(function(row) {
        var p = row.querySelector('.data-param-selector').value;
        var o = row.querySelector('.data-operator-selector').value;
        var v = row.querySelector('.data-value-input').value.trim();
        if (v) { conditions.push({ parameter: p, operator: o, value: v }); }
    });

    if (conditions.length === 0) { alert("Please include at least one valid condition clause."); return; }

    try {
        var postBodyPayload = {
            rule_name: name,
            rule_description: desc,
            conditions: conditions
        };
        
        var res = await fetch('/api/save-rule', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(postBodyPayload)
        });
        
        if (res.status === 201 || res.status === 200) {
            document.getElementById('rightWorkspaceWorkspaceCanvas').innerHTML = '<div class="empty-state-notice">Select or Add a Rule</div>';
            await fetchSystemRulesFromDatabaseQueue();
        } else {
            alert("Database failed to process new rule profile payload rules.");
        }
    } catch(err) {
        console.error("Network interface rule submission trace exception:", err);
    }
}

fetchSystemRulesFromDatabaseQueue();
