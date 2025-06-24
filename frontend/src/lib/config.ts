// lib/config.ts
export const datasetConfig = {
    title: 'Public Comments on "Schedule F" Regulation',
    description: 'Comments about the proposed Schedule Policy/Career Regulation - Improving Performance, Accountability and Responsiveness in the Civil Service',
    subtitle: 'See my git repo', 
    datasetName: 'Comments',
    keyField: 'id',
    
    // Theme options
    theme: {
      colorScheme: 'slate',
      buttonStyle: 'flat',
      cardStyle: 'minimal'
    },
    
    fields: [
      { 
        key: 'title',         
        title: 'Title',         
        filter: 'text', 
        visible: true 
      },
      {
        key: 'stance',
        title: 'Stance',
        filter: 'select',
        visible: true,
        badges: {
          'For': 'bg-success',
          'Against': 'bg-danger',
          'Neutral/Unclear': 'bg-info'
        }
      },
      { 
        key: 'key_quote',       
        title: 'Key Quote',       
        filter: 'text',
        visible: true,
        charLimit: 150
      },
      { 
        key: 'themes',       
        title: 'Themes',    
        filter: 'multi-label',
        format: 'multi-label', 
        visible: true 
      },
      { 
        key: 'rationale',
        title: 'Rationale',
        filter: 'text',
        visible: true,
        charLimit: 200
      },
      { 
        key: 'comment',   
        title: 'Comment',   
        filter: 'text', 
        visible: true,
        charLimit: 400
      },
      {
        key: 'category',
        title: 'Category',
        filter: 'select',
        visible: true,
        badges: {
          'Agency': 'bg-success',
          'Citizen': 'bg-info',
          'Union': 'bg-warning',
          'Professional Organization': 'bg-danger'
        }
      },
      {
        key: 'comment_count',
        title: 'Duplicates',
        filter: 'count',
        format: 'count',
        visible: true,
        badges: {
          '1': 'bg-gray-100',
          '2-10': 'bg-blue-100',
          '11-50': 'bg-purple-100',
          '50+': 'bg-red-100'
        }
      },
      // {
      //   key: 'agencyId',
      //   title: 'Agency ID',
      //   filter: 'text',
      //   visible: false
      // },
      { 
        key: 'link',        
        title: 'Source Link',        
        filter: 'text',
        format: 'link',
        visible: false
      },
      { 
        key: 'id',           
        title: 'Comment ID',           
        visible: false,
        filter: 'text'
      },
      {
        key: 'has_attachments',
        title: 'Has Attachments',
        filter: 'select',
        visible: false,
        badges: {
          'true': 'bg-success',
          'false': 'bg-danger'
        }
      },
      { 
        key: 'posted_date',
        title: 'Posted Date',
        filter: 'date',
        visible: true,
        format: 'date'
      },
      { 
        key: 'received_date',
        title: 'Received Date',
        filter: 'date',
        visible: true,
        format: 'date'
      },
      // {
      //   key: 'commentOn',
      //   title: 'Comment On',
      //   filter: 'text',
      //   visible: false
      // },
      // {
      //   key: 'submitterName',
      //   title: 'Submitter Name',
      //   filter: 'text',
      //   visible: false
      // },
      {
        key: 'organization',
        title: 'Organization',
        filter: 'text',
        visible: true
      },
      // {
      //   key: 'city',
      //   title: 'City',
      //   filter: 'text',
      //   visible: false
      // },
      // {
      //   key: 'state',
      //   title: 'State',
      //   filter: 'text',
      //   visible: false
      // },
      // {
      //   key: 'country',
      //   title: 'Country',
      //   filter: 'text',
      //   visible: false
      // },
      // {
      //   key: 'documentType',
      //   title: 'Document Type',
      //   filter: 'text',
      //   visible: false
      // },
      // {
      //   key: 'attachment_count',
      //   title: 'Attachment Count',
      //   filter: 'count',
      //   format: 'count',
      //   visible: true,
      //   badges: {
      //     '1': 'bg-gray-100',
      //     '2-10': 'bg-blue-100',
      //     '11-50': 'bg-purple-100',
      //     '50+': 'bg-red-100'
      //   }
      // },
      // {
      //   key: 'attachment_urls',
      //   title: 'Attachment URLs',
      //   filter: 'text',
      //   visible: false
      // },
      {
        key: 'attachment_titles',
        title: 'Attachment Titles',
        filter: 'text',
        visible: false
      }
    ],
    
    stats: [
      { 
        key: 'total',        
        label: 'Total Comments',    
        type: 'count' 
      },
      { 
        key: 'stance',       
        label: 'For', 
        type: 'count', 
        match: 'For' 
      },
      { 
        key: 'stance',       
        label: 'Against', 
        type: 'count', 
        match: 'Against' 
      },
      { 
        key: 'stance',       
        label: 'Neutral/Unclear', 
        type: 'count', 
        match: 'Neutral/Unclear' 
      }
    ]
  };
  
  export type Field = typeof datasetConfig.fields[number];
  export type StanceType = 'For' | 'Against' | 'Neutral/Unclear';